package bridge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// The pump is what makes the socket readable while nobody is calling. Before
// it, an event sat on the channel until some call happened to select on it.
func TestThePumpAbsorbsWithNobodyWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	// One wait to open the connection, and nothing else running afterwards.
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)

	eventually(t, "the pump to take the reaction with no call in flight", func() bool {
		return b.pendingReactionCount() == 1
	})
}

// The ordering guarantee, with a question running: receiving a mention and
// registering the thread it opens are one step now, so a reaction on that
// mention cannot be judged in between and dropped. The scenario is run several
// times because what it is guarding against is a scheduling race.
func TestAMentionAndItsReactionSurviveAConcurrentAsk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for attempt := 0; attempt < 10; attempt++ {
		b, api, stream := mentionBridge(ctx, t)
		api.mu.Lock()
		api.questionTS = askTS
		api.mu.Unlock()

		asked := make(chan AskResult, 1)
		go func() {
			// Interruption off, so the mention below cannot end the question
			// early and change which call hands it over.
			result, err := b.Ask(ctx, AskRequest{
				Question:          "Ship it?",
				Options:           []string{"Yes", "No"},
				Timeout:           MinWaitTimeout,
				InterruptDisabled: true,
			})
			if err != nil {
				t.Errorf("Ask() error = %v", err)
			}
			asked <- result
		}()

		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })

		// The mention opens a conversation; the reaction lands on the very
		// message that opened it.
		send(stream, otherChannel, "200.000100", "", mention("ship it?"))
		react(stream, otherChannel, "200.000100", colleague, "white_check_mark", true)

		stream.interactions <- click(testOwner, askTS, 0)

		// A settled question hands over what arrived while it was up, so the
		// mention comes back here; the reaction is slack_wait's to deliver.
		answer := <-asked
		if len(answer.Messages) != 1 {
			t.Fatalf("attempt %d: the question returned %v, want the mention that arrived while it was up", attempt, texts(answer.Messages))
		}

		result := waitOnce(ctx, t, b)
		if len(result.Reactions) != 1 {
			t.Fatalf("attempt %d: delivered %d reactions, want the vote on the message that opened the conversation", attempt, len(result.Reactions))
		}
		if err := b.Close(); err != nil {
			t.Fatalf("attempt %d: Close() error = %v", attempt, err)
		}
	}
}

// A connection that dies while catch-up is fetching history must not take the
// delivery with it. What history returned and what the socket had already
// delivered are both in hand; the disconnection is reported by the call after
// them, not instead of them.
func TestADisconnectDuringCatchUpStillDeliversWhatWasInHand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	gate := make(chan struct{})
	api := &fakeAPI{
		botUserID:   testBotUser,
		historyGate: gate,
		channelHistory: map[string][]candidate{
			testChannel: {
				ownerMsg("100.000100", "already answered"),
				ownerMsg("100.000200", "sent while the socket was dying"),
			},
		},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	done := make(chan WaitResult, 1)
	go func() {
		result, err := b.Wait(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Wait() error = %v, want the message delivered rather than the disconnection reported", err)
		}
		done <- result
	}()

	// The socket dies while history is in flight.
	eventually(t, "catch-up to reach Slack", func() bool { return len(api.calls()) > 0 })
	close(stream.events)
	close(gate)

	select {
	case result := <-done:
		if len(result.Messages) != 1 || result.Messages[0].TS != "100.000200" {
			t.Fatalf("Wait() returned %v, want the message catch-up was holding", texts(result.Messages))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() never returned")
	}

	// And the disconnection is reported next, rather than swallowed.
	if _, err := b.Wait(ctx, 5*time.Second); err == nil {
		t.Error("Wait() = nil error on a dead connection, want it reported once the batch was handed over")
	}
}

// The ordering rule itself: the messages waiting on the wire are applied before
// a reaction the pump is carrying, so the mention that brings a channel into
// scope is registered before the vote on it is judged.
//
// Each step is one lock, and a reaction is carried until a sweep finds the
// events channel empty — so a call draining the queues cannot see a reaction
// that a message ahead of it has not had its say in.
func TestApplyingAReactionAppliesTheMessagesAheadOfIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)

	// Open the connection, so the bridge knows its own user ID and can
	// recognise the mention below.
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	// A channel of its own, so what is read here is only what this test put
	// there: the pump has the connection's.
	events := make(chan StreamEvent, 1)
	events <- StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "200.000100", Channel: otherChannel, User: testOwner, Text: mention("ship it?"),
	}}

	// One step, the way the pump does it: the messages waiting, then the
	// reaction it was carrying, under one lock.
	b.applyReady(generation, events, nil, []Reaction{{
		TS: "200.000100", Channel: otherChannel, User: colleague, Reaction: "white_check_mark", Added: true,
	}})

	if kept := b.drainReactions(); len(kept) != 1 {
		t.Fatalf("drainReactions() = %+v, want the vote: the mention ahead of it opens the conversation it is in", kept)
	}
}

// The pump applies what the socket delivers as it arrives, which can be before
// a question has subscribed to hear about it. A message already queued has to
// interrupt the question anyway, or the notification it would have arrived on
// is one nobody was listening for and the question waits out its whole timeout.
func TestAQuestionIsInterruptedByAMessageQueuedBeforeItSubscribed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	// Connect first, so the pump is already running, and then wait for it to
	// have taken the message. Its notification is spent by the time Ask
	// subscribes, which is the interleaving this is about — leaving the
	// question with nothing left to wake it.
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	stream.events <- StreamEvent{
		Kind:    StreamMessage,
		Message: Message{TS: "100.000200", User: testOwner, Text: "never mind, do this instead"},
	}
	eventually(t, "the pump to take the message", func() bool { return b.Status().PendingBacklogCount > 0 })

	done := make(chan AskResult, 1)
	go func() {
		result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout})
		if err != nil {
			t.Errorf("Ask() error = %v", err)
		}
		done <- result
	}()

	select {
	case result := <-done:
		if !result.Interrupted {
			t.Errorf("Ask() interrupted = false, want the queued message to have ended the question")
		}
		if len(result.Messages) != 1 {
			t.Errorf("Ask() returned %v, want the message it was interrupted by", texts(result.Messages))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Ask() never returned; the message was applied before anything was listening for it")
	}
}

// The queue the pump fills has an end. Before it, the socket's own buffer was
// the limit, because nothing moved a message off it until a call asked; the
// pump moves every one, so a session left working while a channel is busy would
// otherwise grow the heap without bound.
//
// What is already queued stays, and the newest is refused instead: a message
// refused is one history still has, while a message discarded from the queue
// may be a thread reply that catch-up outside the home channel cannot promise
// to find again.
func TestTheMessageQueueFallsBackToHistoryWhenItFills(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// A conversation outside the home channel, and a reply in it. Those replies
	// are the ones history is least able to give back — that catch-up is best
	// effort and stands down entirely when a scope is missing — so they are the
	// ones that must not be discarded to make room.
	b.absorb(StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "200.000100", Channel: otherChannel, User: testOwner, Text: mention("take a look"),
	}})
	b.absorb(StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "200.000200", ThreadTS: "200.000100", Channel: otherChannel, User: testOwner, Text: "in the thread",
	}})
	for i := 0; i <= maxPendingMessages; i++ {
		b.absorb(StreamEvent{Kind: StreamMessage, Message: Message{
			TS: fmt.Sprintf("100.%06d", i+1000), Channel: testChannel, User: testOwner, Text: "noise",
		}})
	}

	// Each queue is counted apart, so the bound is per kind: the home channel's
	// flood cannot crowd out the conversation elsewhere.
	if got := b.pendingHomeCount(); got > maxPendingMessages {
		t.Errorf("pending home messages = %d, want it bounded at %d", got, maxPendingMessages)
	}
	if !b.catchUpDue() {
		t.Error("the queue filled without asking for a catch-up; the refused messages would be lost rather than re-read")
	}
	if b.pendingThreadCount() != 2 {
		t.Error("the thread reply was discarded to make room; history's catch-up outside the home channel is best effort and may never bring it back")
	}
}

// pendingHomeCount reports how many home-channel messages are waiting.
func (b *Bridge) pendingHomeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// pendingThreadCount reports how many replies from conversations outside the
// home channel are waiting.
func (b *Bridge) pendingThreadCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pendingThreads)
}

// catchUpDue reports whether the next call will re-read the window from
// history, which is how a dropped in-memory backlog is recovered.
func (b *Bridge) catchUpDue() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.needCatchUp
}

// The connection hands over what it had before it reports that it is gone, so
// the owner's click can be waiting when the closure is noticed. It is an answer
// whatever happened to the socket afterwards.
func TestAClickThatArrivedWithTheDisconnectionIsStillAnAnswer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	go func() {
		waitForQuestion(b)
		// Queued, then the socket dies: endStream routes the click and records
		// the closure, and both reach the question together.
		stream.interactions <- click(testOwner, askTS, 1)
		close(stream.events)
	}()

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout})
	if err != nil {
		t.Fatalf("Ask() error = %v, want the click honoured rather than the disconnection reported", err)
	}
	if result.ChoiceIndex != 1 {
		t.Errorf("Ask() = %+v, want the option the owner actually clicked", result)
	}
}

// A pump whose connection is replaced, or whose session ends, stops reading and
// says so. Otherwise a call blocked on a context of its own waits out its whole
// timeout on a stream with nobody reading it.
func TestACancelledPumpReportsThatNobodyIsReading(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	if !b.Status().Connected {
		t.Fatal("the bridge reports no connection, so there is no pump to cancel")
	}

	b.stopTheConnection()

	eventually(t, "the pump to record that it has stopped reading", func() bool {
		return !b.Status().Connected
	})
}

// stopTheConnection ends the current connection the way a replacement or a
// shutdown does, taking its pump with it.
func (b *Bridge) stopTheConnection() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopConnectionLocked()
}

// A catch-up asked for while another is in flight has to survive it. The one in
// flight went to Slack with the old window in mind, and clearing the flag on
// its way back would answer a request it never saw — leaving a reconnect, or a
// message refused for want of room, with nothing to fetch it.
func TestACatchUpRequestedMidFlightIsNotSwallowed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	gate := make(chan struct{})
	api := &fakeAPI{
		botUserID:      testBotUser,
		historyGate:    gate,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "already answered")}},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
			t.Errorf("Wait() error = %v", err)
		}
	}()

	// The reconnect lands while the first catch-up is still fetching history.
	eventually(t, "catch-up to reach Slack", func() bool { return len(api.calls()) > 0 })
	b.absorb(StreamEvent{Kind: StreamConnected})
	close(gate)
	<-done

	// The proof is that the window was read again. Swallowed, the first
	// catch-up's single pass would be the only one.
	reads := 0
	for _, req := range api.calls() {
		if req.Channel == testChannel {
			reads++
		}
	}
	// One catch-up reads the channel twice — the window, then the pass that
	// looks for threads talked in since the cursor — so a second catch-up is
	// the difference between two reads and four.
	if reads < 3 {
		t.Errorf("read the home channel %d times, want a second catch-up: the request made while history was in flight was cleared by it", reads)
	}
}

// The first run against a channel seeds the cursor from what is already there,
// so that a fresh install joins the conversation rather than replaying it. A
// message the pump takes from the socket while that read is in flight is not
// part of what was already there: the owner sent it to this session, and
// filtering it against the seed would lose it.
func TestAMessageArrivingDuringTheInitialSeedIsDelivered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	gate := make(chan struct{})
	api := &fakeAPI{
		botUserID:   testBotUser,
		historyGate: gate,
		channelHistory: map[string][]candidate{
			testChannel: {ownerMsg("100.000100", "before this session"), ownerMsg("100.000200", "also before")},
		},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	done := make(chan WaitResult, 1)
	go func() {
		result, err := b.Wait(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Wait() error = %v", err)
		}
		done <- result
	}()

	eventually(t, "the seed to reach Slack", func() bool { return len(api.calls()) > 0 })
	send(stream, testChannel, "100.000150", "", "sent while the cursor was being seeded")
	// Taken by the pump before the seed comes back, which is the case this is
	// about: the message is in hand when the cursor is established.
	eventually(t, "the pump to take it", func() bool { return b.Status().PendingBacklogCount > 0 })
	close(gate)

	select {
	case result := <-done:
		if len(result.Messages) != 1 || result.Messages[0].TS != "100.000150" {
			t.Fatalf("Wait() returned %v, want the message that arrived live during the seed", texts(result.Messages))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() never returned")
	}
}

// Cancelling a connection does not stop its pump the instant it is called: the
// goroutine can be inside a select with a buffered channel ready. What it must
// not do is apply one more event after a replacement connection is installed —
// the single owner would then be two of them, writing into the same queues.
func TestAReplacedConnectionsPumpAppliesNothingMore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "already answered")}},
	}
	connector := &reconnectingConnector{api: api, streams: []*fakeStream{newFakeStream(), newFakeStream()}}
	b := New(ctx, cfg, connector)
	t.Cleanup(func() { _ = b.Close() })

	old := connector.streams[0]
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// The connection dies, its pump records it, and the next call opens the
	// replacement.
	close(old.events)
	eventually(t, "the pump to record the disconnection", func() bool { return !b.Status().Connected })
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on the replacement", result)
	}
	if !b.Status().Connected {
		t.Fatal("the replacement connection was never opened")
	}

	// Anything still on the old stream belongs to a connection nobody owns.
	old.reactions <- Reaction{TS: "100.000500", Channel: testChannel, User: colleague, Reaction: "tada", Added: true}
	send(connector.streams[1], testChannel, "100.000600", "", "on the live connection")

	result := waitOnce(ctx, t, b)
	if len(result.Messages) != 1 || result.Messages[0].TS != "100.000600" {
		t.Fatalf("Wait() returned %v, want only what the live connection delivered", texts(result.Messages))
	}
	if len(result.Reactions) != 0 {
		t.Errorf("Wait() returned %+v from a connection that had been replaced", result.Reactions)
	}
}

// reconnectingConnector hands out a fresh stream for each connection, the way
// the real one does.
type reconnectingConnector struct {
	api     *fakeAPI
	streams []*fakeStream

	mu    sync.Mutex
	calls int
}

func (c *reconnectingConnector) Connect(context.Context, Config) (API, Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream := c.streams[len(c.streams)-1]
	if c.calls < len(c.streams) {
		stream = c.streams[c.calls]
	}
	c.calls++
	return c.api, stream, nil
}

// currentGeneration reports which connection the bridge is on, for tests that
// drive the pump's helpers directly.
func (b *Bridge) currentGeneration() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.connGeneration
}

// A click from a connection that has been replaced is an answer to a question
// that is over. Letting it through could answer the next one instead.
func TestAClickFromAReplacedConnectionIsIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)

	done := make(chan AskResult, 1)
	go func() {
		result, err := b.Ask(ctx, AskRequest{
			Question: "Deploy now?", Options: []string{"Yes", "No"},
			Timeout: 300 * time.Millisecond, InterruptDisabled: true,
		})
		if err != nil {
			t.Errorf("Ask() error = %v", err)
		}
		done <- result
	}()
	waitForQuestion(b)

	// A click arriving from the connection before this one.
	b.applyClick(b.currentGeneration()-1, click(testOwner, askTS, 0))

	result := <-done
	if !result.TimedOut {
		t.Errorf("Ask() = %+v, want the question to time out: the click belonged to a connection that is over", result)
	}
}

// Cancelling a call cancels the session's context in the usual arrangement,
// which ends the pump, which says the connection is gone. The answer to a
// caller that gave up is still that it gave up: the disconnection is the
// consequence, not the cause.
func TestACancelledWaitReportsTheCancellationNotTheDisconnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}
	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "already answered")}},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	done := make(chan error, 1)
	go func() {
		_, err := b.Wait(ctx, 10*time.Second)
		done <- err
	}()
	eventually(t, "the wait to be listening", func() bool { return b.activeWaitCount() > 0 })
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Wait() error = %v, want the cancellation the caller asked for", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() never returned after its context was cancelled")
	}
}

// activeWaitCount reports how many calls are listening, so a test can tell that
// a wait has started before changing what it is waiting for.
func (b *Bridge) activeWaitCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.activeWaits
}

// The state file is replaced by a rename, and a rename can be refused for
// reasons that pass — another process reading it, most of all. A cursor that
// did not land waits and goes again rather than being lost to a moment.
func TestARefusedStateWriteIsTriedAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	b := New(ctx, cfg, &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	// A file where the store needs a directory, so every attempt is refused.
	blocked := filepath.Join(cfg.StateDir, "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("blocking the state directory: %v", err)
	}

	b.mu.Lock()
	b.store = NewStore(filepath.Join(blocked, "state"))
	b.recordStateWriteLocked(stateWrite{
		stateKey: stateKey{kind: writeLastTS, channel: testChannel},
		ts:       "100.000200",
	})
	b.mu.Unlock()

	eventually(t, "the refused cursor to be waiting for another attempt", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		_, kept := b.stateDirty[stateKey{kind: writeLastTS, channel: testChannel}]
		return kept
	})
}
