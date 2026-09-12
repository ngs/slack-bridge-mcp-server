package bridge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
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
	stream.closeEvents()
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

	if kept := b.drainReactions(b.currentGeneration()); len(kept) != 1 {
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
		stream.closeEvents()
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

	// A hole lands while the first catch-up is still fetching history. The
	// socket refusing a message rather than a reconnect, because a
	// connection's own first hello is the catch-up it already asked for
	// arriving, not a hole on top of it.
	eventually(t, "catch-up to reach Slack", func() bool { return len(api.calls()) > 0 })
	b.absorb(StreamEvent{Kind: StreamDropped})
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
	old.closeEvents()
	eventually(t, "the pump to record the disconnection", func() bool { return !b.Status().Connected })
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on the replacement", result)
	}
	if !b.Status().Connected {
		t.Fatal("the replacement connection was never opened")
	}

	// The old connection is over: its stream was closed with it, and what
	// reaches the agent from here is the replacement's.
	send(connector.streams[1], testChannel, "100.000600", "", "on the live connection")

	result := waitOnce(ctx, t, b)
	if len(result.Messages) != 1 || result.Messages[0].TS != "100.000600" {
		t.Fatalf("Wait() returned %v, want what the live connection delivered", texts(result.Messages))
	}
	if len(result.Reactions) != 0 {
		t.Errorf("Wait() returned %+v, want nothing from the connection that was replaced", result.Reactions)
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

func (c *reconnectingConnector) Connect(ctx context.Context, _ Config) (API, Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream := c.streams[len(c.streams)-1]
	if c.calls < len(c.streams) {
		stream = c.streams[c.calls]
	}
	c.calls++
	// Closed with its connection, as the real one is.
	go func() {
		<-ctx.Done()
		stream.closeAll()
	}()
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

	if result := <-done; !result.TimedOut {
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
	// And settled into its sleep. The count goes up before the first look at
	// the queues, and a loss recorded before that look is one the wait finds
	// for itself — which is not what this test is about.
	time.Sleep(200 * time.Millisecond)
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

	// Two attempts, not one: the cursor being in the map proves nothing on its
	// own, since that is where it was put before the writer ever saw it. What
	// is being tested is that a refusal is tried again.
	eventually(t, "the refused cursor to be written a second time", func() bool {
		return b.stateWriteAttempts.Load() >= 2
	})
	// Polled rather than read once: between being taken from the map and being
	// put back there is a moment when the cursor is in the writer's hands and
	// in no queue at all.
	eventually(t, "the refused cursor to be waiting for another attempt", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		_, kept := b.stateDirty[stateKey{kind: writeLastTS, channel: testChannel}]
		return kept
	})
}

// A connection that has already been replaced still lost what it lost. Its
// reactions are in no history and will not be delivered by anybody, so the
// agent is told to read the tally rather than left with a count it cannot know
// is wrong.
func TestAReplacedConnectionsLostReactionsAreStillReported(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// A reaction still in the hands of a connection nobody owns any more.
	reactions := make(chan Reaction, 1)
	reactions <- Reaction{TS: "100.000500", Channel: testChannel, User: colleague, Reaction: "tada", Added: true}
	stale := b.currentGeneration() - 1

	b.endStream(stale, b.currentStream(), nil, nil, nil, reactions, nil, true)

	if !b.droppedReactionMark() {
		t.Error("a reaction received on a replaced connection was dropped with nothing said; the agent's count is wrong and it cannot know")
	}
}

// A call on a connection since replaced is about to be told so, and reports
// nothing. Spending the loss marker on it would leave the next live wait saying
// a loss never happened.
func TestAStaleCallDoesNotConsumeTheLossMarker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	b.mu.Lock()
	b.reactionsDropped = true
	live := b.connGeneration
	b.mu.Unlock()

	if b.takeReactionsDropped(live - 1) {
		t.Error("a stale call reported the loss, which is not its to report")
	}
	if !b.takeReactionsDropped(live) {
		t.Error("the live call was told nothing was lost; the stale one had spent the marker")
	}
}

// The queue a call drains belongs to whichever connection is current. A call on
// one that has been replaced must not take the replacement's reactions: the
// call that wants them would never see them.
func TestAStaleCallDrainsNoReactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)
	eventually(t, "the pump to take the reaction", func() bool { return b.pendingReactionCount() == 1 })

	live := b.currentGeneration()
	if kept := b.drainReactions(live - 1); len(kept) != 0 {
		t.Errorf("a stale call drained %+v, which belongs to the connection that replaced it", kept)
	}
	if kept := b.drainReactions(live); len(kept) != 1 {
		t.Errorf("the live call drained %+v, want the reaction still there", kept)
	}
}

// A shutdown makes every call in flight stale, so a catch-up that comes back
// after it commits nothing. Otherwise its cursor moves in memory while the
// writer that would record it is stopping, and a restart skips the messages
// between.
func TestACatchUpThatReturnsAfterCloseCommitsNothing(t *testing.T) {
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
			testChannel: {ownerMsg("100.000100", "already answered"), ownerMsg("100.000200", "while it was closing")},
		},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = b.Wait(ctx, 5*time.Second)
	}()

	eventually(t, "catch-up to reach Slack", func() bool { return len(api.calls()) > 0 })
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	close(gate)
	<-done

	if got := b.Status().LastTS; got != "100.000100" {
		t.Errorf("last_ts = %q, want the cursor left where it was: the catch-up that moved it could no longer record it", got)
	}
}

// Close is the end of the session, not of a connection. A call still running
// must not open another one behind it, or a pump would be left reading into a
// bridge whose state writer has stopped.
func TestNothingConnectsAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := b.Wait(ctx, 50*time.Millisecond); err == nil {
		t.Error("Wait() = nil error after Close, want it refused rather than opening a connection nothing is winding down")
	}
	if b.Status().Connected {
		t.Error("the bridge reports a connection after Close")
	}
}

// A catch-up that commits belongs to the connection its caller started on. A
// wait whose connection was replaced must not take the replacement's messages:
// the call that wants them would never see them, and the cursor would move
// without them being delivered.
func TestAStaleWaitCommitsNoMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	send(stream, testChannel, "100.000200", "", "for the live connection")
	eventually(t, "the pump to take the message", func() bool { return b.Status().PendingBacklogCount == 1 })

	live := b.currentGeneration()
	msgs, _, err := b.drainCatchUp(ctx, live-1, true, time.Second)
	if err != nil {
		t.Fatalf("drainCatchUp() error = %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("a stale call drained %v, which belongs to the connection that replaced it", texts(msgs))
	}
	if got := b.Status().PendingBacklogCount; got != 1 {
		t.Errorf("pending backlog = %d, want the message left for the live call", got)
	}
}

// An ordinary reconnect abandons nothing. Saying a reaction was lost every time
// the socket came back would send the agent to re-read a tally that was never
// wrong.
func TestAnOrdinaryReconnectReportsNoLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// A connection replaced with nothing in flight on it.
	stale := b.currentGeneration()
	b.mu.Lock()
	b.connGeneration++
	b.mu.Unlock()
	b.endStream(stale, b.currentStream(), nil, nil, nil, nil, nil, true)

	if b.droppedReactionMark() {
		t.Error("an ordinary reconnect reported a lost reaction; nothing was in flight to lose")
	}
}

// A message the pump was handed and a reaction it was carrying go in under one
// lock. Left for the next turn, the carried one would arrive a moment after the
// message, and a call draining between them would see half of it.
func TestAnEventAppliesTheCarriedReactionWithIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	events := make(chan StreamEvent, 1)
	carried := []Reaction{{
		TS: "100.000200", Channel: testChannel, User: colleague, Reaction: "white_check_mark", Added: true,
	}}
	evt := StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "100.000200", Channel: testChannel, User: testOwner, Text: "ship it?",
	}}

	if keep := b.applyEvent(generation, evt, events, nil, carried); len(keep) != 0 {
		t.Fatalf("applyEvent() carried %+v on, want the reaction applied with the message", keep)
	}
	if got := b.Status().PendingBacklogCount; got != 1 {
		t.Fatalf("pending messages = %d, want the message applied", got)
	}
	if kept := b.drainReactions(generation); len(kept) != 1 {
		t.Errorf("drainReactions() = %+v, want the carried reaction applied with the message it arrived with", kept)
	}
}

// A hole found while a catch-up was fetching throws that whole pass away.
// Nobody knows where the hole is, so nothing the pass read can be trusted to
// belong after it: the queues are left as they are, the cursor does not move,
// and the request stands for the next call, which reads the window with the
// hole already in the past.
func TestAHoleDuringACatchUpThrowsThePassAway(t *testing.T) {
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
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	// Connect first, so there is a generation and a cursor to work from, and
	// only then hold history open.
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	gate := make(chan struct{})
	api.mu.Lock()
	api.historyGate = gate
	calls := len(api.historyCalls)
	api.mu.Unlock()

	b.mu.Lock()
	b.needCatchUp = true
	b.mu.Unlock()

	generation := b.currentGeneration()
	type batch struct {
		msgs []Message
		err  error
	}
	done := make(chan batch, 1)
	go func() {
		msgs, _, err := b.drainCatchUp(ctx, generation, true, time.Second)
		done <- batch{msgs, err}
	}()

	// A live message, and then the socket reporting a hole — both while the
	// history request is in flight.
	eventually(t, "catch-up to reach Slack", func() bool { return len(api.calls()) > calls })
	b.absorb(StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "100.000300", Channel: testChannel, User: testOwner, Text: "newer than the hole",
	}})
	b.absorb(StreamEvent{Kind: StreamDropped})
	close(gate)

	got := <-done
	if got.err != nil {
		t.Fatalf("drainCatchUp() error = %v", got.err)
	}
	if len(got.msgs) != 0 {
		t.Fatalf("drainCatchUp() = %v, want nothing: what it read cannot be placed against a hole it did not see", texts(got.msgs))
	}
	if got := b.Status().LastTS; got != "100.000100" {
		t.Errorf("last_ts = %q, want it left behind the hole", got)
	}
	if b.Status().PendingBacklogCount != 1 {
		t.Error("the message behind the hole was dropped rather than kept for the catch-up that covers it")
	}
}

// A click routed by a connection the question never saw is not its answer. The
// question is about to be told its own connection is gone; the click belongs to
// whatever is asked next.
func TestAClickFromALaterConnectionDoesNotAnswerAnOlderQuestion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)

	type outcome struct {
		result AskResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		// An error is expected and not the point: a question whose connection
		// is replaced is told so. The point is what it must not return, which
		// is an answer it never had.
		result, err := b.Ask(ctx, AskRequest{
			Question: "Deploy now?", Options: []string{"Yes", "No"},
			Timeout: 300 * time.Millisecond, InterruptDisabled: true,
		})
		done <- outcome{result, err}
	}()
	waitForQuestion(b)

	// The connection is replaced, and the replacement routes a click that
	// matches the question's own message.
	b.mu.Lock()
	b.connGeneration++
	live := b.connGeneration
	b.mu.Unlock()
	b.applyClick(live, click(testOwner, askTS, 0))

	got := <-done
	if got.err == nil && got.result.ChoiceIndex >= 0 {
		t.Errorf("Ask() = %+v, want no answer: the click came from a connection the question never had", got.result)
	}
}

// The cursor has to move on an ordinary delivery, or every reconnect replays
// what was just handed over.
func TestAnOrdinaryDeliveryAdvancesTheCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	send(stream, testChannel, "100.000200", "", "ship it?")
	if msgs := waitOnce(ctx, t, b).Messages; len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the message", texts(msgs))
	}

	if got := b.Status().LastTS; got != "100.000200" {
		t.Errorf("last_ts = %q, want it moved to the message just handed over", got)
	}
}

// A message refused before the first cursor exists is not recoverable from a
// seed that would filter it out. The cursor goes behind what the session has
// actually seen instead, so the catch-up reads forward from there.
func TestARefusalBeforeTheFirstCursorKeepsTheWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	// The channel's newest message is newer than anything the session has in
	// hand — somebody else posted while this one was connecting — so a seed
	// taken from it would sit ahead of the refused message and hide it.
	api := &fakeAPI{
		botUserID: testBotUser,
		channelHistory: map[string][]candidate{
			testChannel: {ownerMsg("100.000100", "before this session"), ownerMsg("900.000000", "somebody else, just now")},
		},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	// Queued before the connection is opened, so the seed is read with them
	// already in hand, and one more than the queue will take.
	b.mu.Lock()
	for i := 0; i < maxPendingMessages; i++ {
		b.pending = append(b.pending, Message{
			TS: fmt.Sprintf("200.%06d", i), Channel: testChannel, User: testOwner, Text: "queued",
		})
	}
	b.mu.Unlock()
	b.absorb(StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "300.000100", Channel: testChannel, User: testOwner, Text: "refused",
	}})

	waitOnce(ctx, t, b)

	// The seed did not become the cursor. What did is the batch actually handed
	// over, which leaves the refused message inside the window the catch-up
	// asked for will read.
	if got := b.Status().LastTS; !tsLess(got, "300.000100") {
		t.Errorf("last_ts = %q, want it behind the refused message so history still has it", got)
	}

}

// A window bigger than one catch-up can read is read as far as it goes, and
// said so. The pages it reads are the newest of the window, so what it did not
// reach is older than everything delivered: no later pass can get back to it,
// and asking for one would only read the same pages again.
func TestACatchUpBiggerThanOnePassDeliversWhatItReached(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000000"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	// More messages than one catch-up can page through.
	history := make([]candidate, 0, maxHistoryPages*historyPageLimit+10)
	for i := 0; i < cap(history); i++ {
		history = append(history, ownerMsg(fmt.Sprintf("100.%06d", i+1), "backlog"))
	}
	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: history},
	}
	// Without the socket's hello: this is about what one pass reads, and a
	// hello asks for a pass of its own.
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream(), quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	msgs := waitOnce(ctx, t, b).Messages
	if len(msgs) != maxHistoryPages*historyPageLimit {
		t.Fatalf("Wait() returned %d messages, want the %d one pass reads", len(msgs), maxHistoryPages*historyPageLimit)
	}
	// The newest of the window, and the cursor with them: the next call starts
	// from there rather than reading the same pages again for ever.
	if got := b.Status().LastTS; got != msgs[len(msgs)-1].TS {
		t.Errorf("last_ts = %q, want the newest message handed over (%q)", got, msgs[len(msgs)-1].TS)
	}
	if b.catchUpDue() {
		t.Error("another catch-up was asked for, which would read the same newest pages again and make no progress")
	}
}

// A catch-up whose connection is replaced under it is reading an installation
// that no longer exists, and nothing it finds will be committed. Until it
// returns it holds the catch-up slot, which is what the connection that
// replaced it is waiting for before it can read its own backlog — so it ends
// with the connection rather than with the tool call that started it.
func TestACatchUpEndsWithTheConnectionItStartedOn(t *testing.T) {
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
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// Held open for the rest of the test: what ends the fetch has to be the
	// connection, not the gate.
	gate := make(chan struct{})
	api.mu.Lock()
	api.historyGate = gate
	calls := len(api.historyCalls)
	api.mu.Unlock()

	b.mu.Lock()
	b.needCatchUp = true
	b.mu.Unlock()

	generation := b.currentGeneration()
	type batch struct {
		msgs []Message
		err  error
	}
	done := make(chan batch, 1)
	go func() {
		// A generous slot wait: the point is that the fetch itself gives up,
		// not that somebody timed out waiting for a turn.
		msgs, _, err := b.drainCatchUp(context.Background(), generation, true, 10*time.Second)
		done <- batch{msgs, err}
	}()

	eventually(t, "the catch-up to be in flight", func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.historyCalls) > calls
	})

	b.forceReconnect()

	select {
	case got := <-done:
		if got.err != nil {
			t.Errorf("drainCatchUp() error = %v, want the abandoned fetch to report nothing", got.err)
		}
		if len(got.msgs) != 0 {
			t.Errorf("drainCatchUp() = %v, want nothing from a connection that has been replaced", texts(got.msgs))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the catch-up is still holding the slot after its connection was replaced")
	}
}

// Sweeping the messages and then the reactions is not by itself an order. One
// goroutine fills both channels, in the order Slack sent things, so a message
// handed over between the two sweeps is taken after the reaction that came
// behind it. A reaction is judged when it is handed over, not when it is taken,
// so letting one into the queue while the messages it came behind are still on
// their channel is what splits the pair: the next call drains the reaction,
// finds no conversation open, and throws it away.
//
// So a sweep that runs out of room carries the reactions on instead of queueing
// them, and they wait for the messages ahead of them.
func TestAReactionWaitsWhileMessagesAreStillOnTheChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// Channels of this test's own: the window between the two sweeps, with a
	// mention still waiting and the reaction that came behind it already taken
	// from the socket.
	events := make(chan StreamEvent, 2)
	for i, ts := range []string{"200.000100", "200.000200"} {
		events <- StreamEvent{Kind: StreamMessage, Message: Message{
			TS: ts, Channel: otherChannel, User: testOwner, Text: mention(fmt.Sprintf("ship %d?", i)),
		}}
	}
	reactions := make(chan Reaction, 1)
	reactions <- Reaction{
		TS: "200.000200", Channel: otherChannel, User: colleague, Reaction: "white_check_mark", Added: true,
	}

	// Room for one of the two messages. The sweep spends it, sees that the
	// channel still has something in it — looking, not taking, because taking
	// would only move the boundary — and that is what makes the reaction wait.
	b.mu.Lock()
	applied, keep := b.takeReactionsLocked(events, reactions, nil, 1)
	b.mu.Unlock()

	if applied != 1 {
		t.Fatalf("takeReactionsLocked() applied %d messages, want the one it had room for", applied)
	}
	if len(keep) != 1 {
		t.Errorf("takeReactionsLocked() carried %+v, want the reaction to wait for the message still on the channel", keep)
	}
	if kept := b.drainReactions(b.currentGeneration()); len(kept) != 0 {
		t.Errorf("drainReactions() = %+v, want nothing: a reaction queued now is judged against a conversation the message ahead of it has not opened", kept)
	}
}

// A reaction left on a channel nobody will read again is in no history, and
// nobody will deliver it. The last sweep of a closing connection takes more
// than an ordinary one for that reason, and what it still cannot take is
// reported so the agent knows to read the tally.
func TestReactionsLeftOnAClosingConnectionAreReported(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	// More than an ordinary sweep takes, which is what an ordinary sweep would
	// have left behind with nothing said.
	const sent = maxSweep + 50
	reactions := make(chan Reaction, sent)
	for i := 0; i < sent; i++ {
		reactions <- Reaction{
			TS:       fmt.Sprintf("100.%06d", i+1),
			Channel:  testChannel,
			User:     colleague,
			Reaction: "eyes",
			Added:    true,
		}
	}

	b.endStream(generation, b.currentStream(), nil, nil, nil, reactions, nil, true)

	if !b.droppedReactionMark() {
		t.Error("reactions were left on a connection that closed with nothing said; the agent's count is wrong and it cannot know")
	}
}

// A channel that was empty when the bridge first looked leaves no cursor
// behind: there is no message to point at. That it was looked at is recorded
// all the same, because without it a restart reads the channel as one it has
// never seen — and takes the first message posted while it was down for the
// channel's past, which is exactly the message it was started to deliver.
func TestAnEmptyChannelIsRememberedAsSeen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	empty := &fakeAPI{botUserID: testBotUser}
	first := New(ctx, cfg, &fakeConnector{api: empty, stream: newFakeStream()})
	if result := waitOnce(ctx, t, first); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on an empty channel", result)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if seeded, err := NewStore(cfg.StateDir).Seeded(testChannel); err != nil || !seeded {
		t.Fatalf("Seeded() = %v (err %v), want the empty channel recorded as looked at", seeded, err)
	}

	// Posted while the session was down, and the first thing in the channel.
	api := &fakeAPI{
		botUserID: testBotUser,
		channelHistory: map[string][]candidate{
			testChannel: {ownerMsg("100.000100", "are you there?")},
		},
	}
	second := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = second.Close() })

	msgs := waitOnce(ctx, t, second).Messages
	if len(msgs) != 1 {
		t.Fatalf("Wait() = %v, want the message posted while the session was down", texts(msgs))
	}
}

// A gap in what has been applied is a mention that may not be there yet. A
// reaction that matches nothing while a catch-up is outstanding is held for it
// rather than judged against a conversation the session has not read its way
// into: the catch-up recovers the mention, and nothing recovers the reaction.
func TestAReactionWaitsForTheCatchUpThatExplainsIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	// The socket overflowed: the mention that opened a conversation in the
	// other channel is in the hole, and this reaction is on a message from it.
	b.mu.Lock()
	b.needCatchUp = true
	b.absorbReactionLocked(Reaction{
		TS: "200.000100", Channel: otherChannel, User: colleague, Reaction: "eyes", Added: true,
	})
	b.mu.Unlock()

	if kept := b.drainReactions(generation); len(kept) != 0 {
		t.Fatalf("drainReactions() = %+v, want nothing yet: the conversation it belongs to has not been read in", kept)
	}

	// The catch-up arrives and opens the conversation.
	b.mu.Lock()
	b.absorbLocked(StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "200.000100", Channel: otherChannel, User: testOwner, Text: mention("ship it?"),
	}})
	b.needCatchUp = false
	b.mu.Unlock()

	if kept := b.drainReactions(generation); len(kept) != 1 {
		t.Errorf("drainReactions() = %+v, want the reaction the catch-up explained", kept)
	}
}

// The closing sweep of the messages is bounded like every other. A connection
// that closes with more messages waiting than it can take leaves reactions that
// would be judged against a connection only half applied — so they are reported
// as lost instead, which the agent can still read the tally for.
func TestReactionsAreReportedWhenTheClosingSweepCannotFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	// More than the teardown will take altogether, so the channel cannot be
	// emptied and the reactions have messages ahead of them that will never be
	// applied.
	const sent = maxSweep + maxTeardownSweep + 1
	events := make(chan StreamEvent, sent)
	for i := 0; i < sent; i++ {
		events <- StreamEvent{Kind: StreamMessage, Message: Message{
			TS: fmt.Sprintf("300.%06d", i+1), Channel: testChannel, User: testOwner, Text: "flood",
		}}
	}
	reactions := make(chan Reaction, 1)
	reactions <- Reaction{
		TS: "300.000001", Channel: testChannel, User: colleague, Reaction: "eyes", Added: true,
	}

	b.endStream(generation, b.currentStream(), nil, events, nil, reactions, nil, true)

	if !b.droppedReactionMark() {
		t.Error("a reaction was judged against a connection whose messages could not all be applied, with nothing said")
	}
}

// The timeout a wait is given is a promise about when it answers, and a catch-up
// that has its turn but not an answer from Slack must not break it. The fetch
// is bounded by the same deadline, and a fetch cut short is the ordinary empty
// answer: nothing is committed until the messages are handed over, so the next
// call reads the same window.
func TestAWaitAnswersOnTimeEvenWithSlackNotAnswering(t *testing.T) {
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
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// Slack stops answering, and a catch-up is asked for.
	gate := make(chan struct{})
	defer close(gate)
	api.mu.Lock()
	api.historyGate = gate
	api.mu.Unlock()

	b.mu.Lock()
	b.needCatchUp = true
	b.mu.Unlock()

	type answer struct {
		result WaitResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := b.Wait(ctx, 200*time.Millisecond)
		done <- answer{result, err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Wait() error = %v, want the ordinary timed-out answer", got.err)
		}
		if !got.result.TimedOut {
			t.Errorf("Wait() = %+v, want it to report the timeout it promised", got.result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() never came back: its own deadline did not bound the fetch it was waiting on")
	}
}

// A message refused for want of room is announced as an event on the channel
// that had no room, so the announcement waits for the flood to pass. Until it
// arrives the bridge would believe it had missed nothing, and a reaction judged
// in that window is judged against the conversations the refused message would
// have opened. The pump asks the stream instead of waiting to be told.
func TestAnOverflowIsSeenBeforeTheStreamCanAnnounceIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// Refused, and no room to say so.
	stream.pendingOverflow.Store(true)
	// Anything at all, to bring the pump round again.
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "100.000300", Channel: testChannel, User: testOwner, Text: "the one that fitted",
	}}

	eventually(t, "the refused message to be asked for", func() bool { return b.catchUpDue() })
}

// A lost reaction is news in itself. The stream's marker used to be read only
// by a call that happened to come round, so a wait blocked on a long timeout
// could sit out the whole of it holding a count it had been told nothing about.
func TestALostReactionWakesTheWaitThatIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	type answer struct {
		result WaitResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := b.Wait(ctx, 10*time.Second)
		done <- answer{result, err}
	}()
	eventually(t, "the wait to be listening", func() bool { return b.activeWaitCount() > 0 })
	// And settled into its sleep. The count goes up before the first look at
	// the queues, and a loss recorded before that look is one the wait finds
	// for itself — which is not what this test is about.
	time.Sleep(200 * time.Millisecond)

	// The reaction queue overflowed on the socket's side while the pump was
	// busy with messages that go nowhere: someone else talking in a channel
	// the session is not in. Nothing they carry reaches a queue, so nothing
	// they carry would wake the wait.
	stream.reactionsDropped.Store(true)
	for i := 0; i < 8; i++ {
		stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{
			TS: fmt.Sprintf("400.%06d", i+1), Channel: otherChannel, User: colleague, Text: "not for us",
		}}
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Wait() error = %v", got.err)
		}
		if !got.result.ReactionsDropped {
			t.Errorf("Wait() = %+v, want the loss reported rather than waited out", got.result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait sat out its timeout holding a loss nobody told it about")
	}
}

// A hole found while the cursor was being seeded throws the pass away, but not
// the seed: where the channel was when this session found it is not something a
// hole changes, and seeding again would read a channel that has moved on. The
// message that arrived while the seed was being read is queued, and it is older
// than the seed — which is exactly why a live message is never filtered by the
// cursor.
func TestAHoleDuringTheSeedKeepsBothTheSeedAndTheMessage(t *testing.T) {
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
	eventually(t, "the pump to take it", func() bool { return b.Status().PendingBacklogCount > 0 })

	// A reconnect while the seed is still out: there is a hole after the
	// window it read, so the queue stays where it is.
	b.absorb(StreamEvent{Kind: StreamConnected})
	close(gate)

	// The pass that found the hole leaves the queue alone and asks for another.
	// That second pass is the one that hands it over.
	select {
	case result := <-done:
		if len(result.Messages) != 1 || result.Messages[0].TS != "100.000150" {
			t.Fatalf("Wait() returned %v, want the message that arrived while the cursor was being seeded", texts(result.Messages))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() never returned")
	}
}

// A seed established by a call whose connection was replaced is kept in memory,
// because that call may not write. The call that takes over may have nothing of
// its own to record — an empty channel, or replies and no messages — and if the
// seed went unwritten with it, a restart would seed again and read everything
// sent while the session was down as the channel's past.
func TestASeedFromAReplacedCallStillReachesTheStateFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	api := &fakeAPI{botUserID: testBotUser}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on an empty channel", result)
	}

	// What a replaced call leaves behind: the seed in memory and the debt for
	// the write it could not make.
	b.mu.Lock()
	b.cursorSeeded = true
	b.lastTS = "100.000900"
	b.seedUnwritten = true
	b.mu.Unlock()

	// A call with nothing at all to hand over.
	if _, _, err := b.drainCatchUp(ctx, b.currentGeneration(), true, time.Second); err != nil {
		t.Fatalf("drainCatchUp() error = %v", err)
	}

	eventuallyOnDisk(t, "the seed to reach the state file", func() bool {
		stored, err := NewStore(cfg.StateDir).LastTS(testChannel)
		return err == nil && stored == "100.000900"
	})
}

// The stream's lost-reaction marker clears when it is read. A connection
// replaced between the question and the answer would otherwise take the answer
// with it, and a reaction lost on a connection that has died is still one the
// agent's count is missing.
func TestALossReadFromAReplacedStreamIsStillRecorded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	stream.reactionsDropped.Store(true)
	b.noteStreamLosses(b.currentGeneration()-1, stream)

	if !b.droppedReactionMark() {
		t.Error("a loss read from a replaced connection was forgotten; the agent's count is wrong and it cannot know")
	}
}

// An overflow is a flag on the stream rather than an event, and the stream can
// only announce one once the channel that had no room has some. A connection
// that then goes quiet would hold the news for as long as the quiet lasted.
func TestAQuietStreamsOverflowIsStillNoticed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// Nothing follows it: no message, no click, no reaction.
	stream.pendingOverflow.Store(true)

	eventually(t, "the refused message to be asked for", func() bool { return b.catchUpDue() })
}

// absorb folds one stream event into the bridge's pending state, taking b.mu
// the way the pump does. It is for tests that drive the queues directly: in
// the bridge itself the pump is the only thing that applies an event, and it
// holds the lock across a whole batch.
func (b *Bridge) absorb(evt StreamEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.absorbLocked(evt)
}

// routeInteraction hands one click on under b.mu, for tests that deliver a
// click without a pump.
func (b *Bridge) routeInteraction(in Interaction) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deliverInteraction(in)
}

// A message refused for want of room is not a hole. The queue was full, so the
// refused message is newer than everything in it, and everything read alongside
// it is still good: the batch is handed over, the cursor moves, and the request
// to read the window again stands for the message that did not fit.
//
// Treated as a hole, this was absorbing. The queue was kept, so it stayed full,
// so the next live message was refused too — and every pass delivered the same
// window again while the queue it duplicated never drained.
func TestARefusalDuringCatchUpStillDrainsTheQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{botUserID: testBotUser}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// The home queue filled to the brim and one more refused, which is what
	// asks for the window to be read.
	var history []candidate
	for i := 0; i <= maxPendingMessages; i++ {
		ts := fmt.Sprintf("100.%06d", i+1000)
		history = append(history, ownerMsg(ts, "flood"))
		b.absorb(StreamEvent{Kind: StreamMessage, Message: Message{
			TS: ts, Channel: testChannel, User: testOwner, Text: "flood",
		}})
	}
	if !b.catchUpDue() || b.pendingHomeCount() != maxPendingMessages {
		t.Fatalf("setup: catch-up due = %v, queued = %d", b.catchUpDue(), b.pendingHomeCount())
	}

	gate := make(chan struct{})
	api.mu.Lock()
	api.history = history
	api.historyGate = gate
	calls := len(api.historyCalls)
	api.mu.Unlock()

	generation := b.currentGeneration()
	first := make(chan []Message, 1)
	go func() {
		msgs, _, err := b.drainCatchUp(ctx, generation, true, 5*time.Second)
		if err != nil {
			t.Errorf("drainCatchUp() error = %v", err)
		}
		first <- msgs
	}()

	eventually(t, "the catch-up to reach Slack", func() bool { return len(api.calls()) > calls })

	// One more owner message while history is in flight. The queue is full, so
	// it is refused — and that refusal used to gap the pass that was about to
	// drain the queue it could not join.
	b.absorb(StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "100.009999", Channel: testChannel, User: testOwner, Text: "refused",
	}})
	close(gate)

	delivered := <-first
	if got := b.pendingHomeCount(); got != 0 {
		t.Fatalf("queued messages = %d after the pass that delivered them, want the queue drained: a full queue refuses the next message, which would gap the next pass in turn", got)
	}

	api.mu.Lock()
	api.historyGate = nil
	api.history = append(history, ownerMsg("100.009999", "refused"))
	api.mu.Unlock()

	// The refusal is still owed a pass, and that pass brings back the message
	// that did not fit — and nothing that was already handed over.
	second, _, err := b.drainCatchUp(ctx, generation, true, 5*time.Second)
	if err != nil {
		t.Fatalf("drainCatchUp() error = %v", err)
	}
	seen := make(map[string]bool, len(delivered))
	for _, m := range delivered {
		seen[m.TS] = true
	}
	duplicates := 0
	for _, m := range second {
		if seen[m.TS] {
			duplicates++
		}
	}
	if duplicates > 0 {
		t.Errorf("%d of the %d messages in the second batch had already been handed over", duplicates, len(second))
	}
	if len(second) != 1 || second[0].TS != "100.009999" {
		t.Errorf("second batch = %v, want only the message that was refused", texts(second))
	}
}

// Slack retries an envelope it has not been acknowledged for, and the receipt
// goes out before the message reaches the stream: a receipt lost on the way
// back brings the same message round again, on this connection or the next.
// The cursor cannot answer for it, since a live message is deliberately not
// filtered by the cursor, so what has been handed over is remembered instead.
func TestARedeliveredMessageIsHandedOverOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	send(stream, testChannel, "100.000300", "", "ship it?")
	if msgs := waitOnce(ctx, t, b).Messages; len(msgs) != 1 {
		t.Fatalf("Wait() = %v, want the message", texts(msgs))
	}

	// The same envelope again, which is what an unacknowledged one comes back
	// as.
	send(stream, testChannel, "100.000300", "", "ship it?")

	if msgs := waitOnce(ctx, t, b).Messages; len(msgs) != 0 {
		t.Errorf("Wait() = %v, want nothing: the owner sent that message once", texts(msgs))
	}
}

// A message refused while a catch-up is already outstanding still raises a
// hole of its own. The catch-up in flight went to Slack before the refusal, so
// it is not the one that answers for it — and on a stream that then goes quiet
// there is no event coming to ask again.
func TestAnOverflowWhileCatchUpIsDueStillAsksAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	b.mu.Lock()
	b.needCatchUp = true
	holes := b.holeEpoch
	b.mu.Unlock()

	stream.pendingOverflow.Store(true)

	eventually(t, "the refusal to be counted as a hole of its own", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.holeEpoch != holes
	})

	// And only once, however long the stream holds it.
	b.mu.Lock()
	raised := b.holeEpoch
	b.mu.Unlock()
	time.Sleep(3 * overflowPollWait)
	b.mu.Lock()
	again := b.holeEpoch
	b.mu.Unlock()
	if again != raised {
		t.Errorf("hole count went from %d to %d while the stream held one overflow, want it raised once", raised, again)
	}
}

// lockProbeStream answers the stream's optional questions and reports whether
// it was asked with the bridge's lock held. A stream is somebody else's code:
// asked under b.mu it could block, or call back into the bridge, and take the
// pump and every tool call waiting on that lock down with it.
type lockProbeStream struct {
	*fakeStream
	b            *Bridge
	askedLocked  atomic.Bool
	askedAtAll   atomic.Bool
	pendingValue bool
}

func (s *lockProbeStream) PendingOverflow() bool {
	s.askedAtAll.Store(true)
	// TryLock rather than Lock, so a probe that finds the lock taken says so
	// instead of joining the deadlock it is looking for.
	if !s.b.mu.TryLock() {
		s.askedLocked.Store(true)
		return s.pendingValue
	}
	s.b.mu.Unlock()
	return s.pendingValue
}

// The bridge asks a stream its questions with the lock let go. Everything else
// about the pump holds b.mu for a whole batch, which is what makes a message
// and its reaction arrive together — and holding it across a call into an
// implementation this package does not own is how that becomes a deadlock.
func TestAStreamIsNeverAskedAnythingUnderTheLock(t *testing.T) {
	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	b := &Bridge{cfg: cfg, catchUpSlot: make(chan struct{}, 1)}
	stream := &lockProbeStream{fakeStream: newFakeStream(), b: b, pendingValue: true}

	b.mu.Lock()
	b.connGeneration = 1
	b.connected = true
	b.mu.Unlock()

	b.endStream(1, stream, nil, stream.events, stream.interactions, stream.reactions, nil, true)

	if !stream.askedAtAll.Load() {
		t.Fatal("the closing connection never asked the stream whether it was holding an overflow")
	}
	if stream.askedLocked.Load() {
		t.Error("the stream was asked with b.mu held; an implementation that blocks there takes the pump and every tool call with it")
	}
}

// The probes from the local review, kept as tests. Each was written against
// the behaviour before this round and failed on it.
// one overflow produces two holes (poll + the StreamDropped announcement).
func TestOneOverflowRaisesOneHole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout", result)
	}

	b.mu.Lock()
	before := b.holeEpoch
	b.mu.Unlock()

	stream.pendingOverflow.Store(true)
	eventually(t, "the overflow to be noticed by the poll", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.holeEpoch != before
	})
	b.mu.Lock()
	afterPoll := b.holeEpoch
	b.mu.Unlock()

	// The stream now announces the same overflow, the way flushDropped does:
	// event first, flag cleared after.
	stream.events <- StreamEvent{Kind: StreamDropped}
	stream.pendingOverflow.Store(false)

	time.Sleep(3 * overflowPollWait)
	b.mu.Lock()
	afterAnnounce := b.holeEpoch
	b.mu.Unlock()
	if afterAnnounce != afterPoll {
		t.Errorf("holeEpoch %d -> %d for a single overflow: the poll and the announcement each raised one", afterPoll, afterAnnounce)
	}
}

// slowAPI makes every history call take a while, so a hole can land while one
// is in flight.
type slowAPI struct {
	*fakeAPI
	delay time.Duration
}

func (s *slowAPI) History(ctx context.Context, req HistoryRequest) (HistoryPage, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return HistoryPage{}, ctx.Err()
	}
	return s.fakeAPI.History(ctx, req)
}

// a stream that keeps reporting holes (reconnect storm) starves the
// wait: the queued live message is never handed over while holes keep coming.
func TestAHoleStormStillHandsOverTheQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatal(err)
	}
	inner := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
	}
	api := &slowAPI{fakeAPI: inner, delay: 60 * time.Millisecond}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: inner, stream: stream})
	t.Cleanup(func() { _ = b.Close() })
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout", result)
	}
	// Swap the API for the slow one under the lock.
	b.mu.Lock()
	b.api = api
	b.mu.Unlock()

	// A message the owner sent, sitting in the queue.
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "100.000200", Channel: testChannel, User: testOwner, Text: "hello?",
	}}
	eventually(t, "the message to be queued", func() bool { return b.pendingHomeCount() == 1 })

	// The socket flaps: a reconnect every 30ms for 1.5s. Each is a hole.
	stormDone := make(chan struct{})
	go func() {
		defer close(stormDone)
		end := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(end) {
			select {
			case stream.events <- StreamEvent{Kind: StreamConnected}:
			case <-ctx.Done():
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()

	start := time.Now()
	result, err := b.Wait(ctx, 5*time.Second)
	took := time.Since(start)
	<-stormDone
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Wait returned after %v: %+v; history calls=%d", took, texts(result.Messages), len(inner.calls()))
	if took > time.Second {
		t.Errorf("a message already in the queue was held for %v while the socket flapped; nothing bounds the discard-and-retry", took)
	}
}

// out-of-scope reactions held while a catch-up is due accumulate to
// the cap and then poison the loss marker.
func TestAHeldReactionNeverReportsALossOfItsOwn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout", result)
	}

	// A busy channel the session is only sitting in, while a catch-up is
	// outstanding: every one of these is held, and none of them is news.
	b.mu.Lock()
	b.needCatchUp = true // e.g. a conversation left unread for want of budget
	for i := 0; i < maxHeldReactions; i++ {
		b.absorbReactionLocked(Reaction{TS: "1.0", Channel: "CELSEWHERE", User: "UOTHER", Reaction: "eyes", Added: true, EventTS: strconv.Itoa(i)})
	}
	got := b.drainReactionsLocked()
	held := len(b.heldReactions)
	dropped := b.reactionsDropped
	b.mu.Unlock()

	if len(got) != 0 {
		t.Fatalf("out-of-scope reactions were delivered: %d", len(got))
	}
	t.Logf("held=%d dropped=%v", held, dropped)
	if held != maxHeldReactions {
		t.Errorf("held reactions = %d, want all of them waiting for the catch-up", held)
	}
	if dropped {
		t.Error("a reaction in a channel the session is not in set reactions_dropped; the agent is told to re-read tallies for nothing")
	}

	// Running out of room for them is different. What goes was being kept
	// because a catch-up might yet put it in scope, so its loss is news.
	b.mu.Lock()
	b.absorbReactionLocked(Reaction{TS: "1.0", Channel: "CELSEWHERE", User: "UOTHER", Reaction: "eyes", Added: true, EventTS: "one too many"})
	b.mu.Unlock()
	b.drainReactions(b.currentGeneration())

	if !b.droppedReactionMark() {
		t.Error("a held reaction was dropped for want of room with nothing said; it may have been a vote the agent never saw")
	}
}

// with more than maxThreadsPerCatchUp conversations open, needCatchUp
// never clears, and every wakeup re-runs the whole scan.
func TestMoreOpenThreadsThanOnePassReadsStillSettles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxThreadsPerCatchUp+1; i++ {
		if err := store.SetThread("CPROJ", "50.00000"+strconv.Itoa(i), "60.000000"); err != nil {
			t.Fatal(err)
		}
	}
	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
	}
	stream := newFakeStream()
	// Without the socket's hello: this counts what the budget rule costs, and
	// a hello brings a pass of its own.
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream, quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout", result)
	}
	api.mu.Lock()
	replies0 := len(api.replyCalls)
	api.mu.Unlock()
	// The first catch-up reads what it has budget for and the walk behind it
	// reads the rest, so by now every conversation has been reached once.
	if replies0 > maxThreadsPerCatchUp+1 {
		t.Fatalf("the first catch-up and the walk after it cost %d thread reads", replies0)
	}
	if b.threadsWaiting() {
		t.Fatalf("a conversation is still waiting after %d reads; the walk that follows a full catch-up should reach the ones it skipped", replies0)
	}

	for i := 0; i < 3; i++ {
		stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{
			TS: "100.00030" + strconv.Itoa(i), Channel: testChannel, User: testOwner, Text: "hi",
		}}
		if got := waitOnce(ctx, t, b); len(got.Messages) != 1 {
			t.Fatalf("Wait() = %+v", got)
		}
	}
	api.mu.Lock()
	replies := len(api.replyCalls) - replies0
	api.mu.Unlock()
	t.Logf("three live messages cost %d conversations.replies calls", replies)
	if replies != 0 {
		t.Errorf("three deliveries cost %d thread reads; nothing was waiting to be read", replies)
	}
}

// threadsWaiting reports whether a conversation was left unread for want of
// budget, which is its own errand rather than a catch-up.
func (b *Bridge) threadsWaiting() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.threadsSkipped
}

// the socket's own Connected event on first connect discards the
// A connection's own hello is the catch-up it already asked for arriving, as
// long as that catch-up has not gone to Slack yet: raising a hole for it would
// throw away the read it is about to make. Once the read has happened the same
// hello means something else: another read, which
// TestAHelloDuringTheFirstReadAsksForOneMorePass and
// TestAMessageSentBeforeTheSocketCameUpIsStillRead cover between them.
func TestAHelloThatBeatsTheFirstReadIsNotAHole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// A connection that has just asked for its catch-up and not yet looked.
	b.mu.Lock()
	b.requestCatchUpForHoleLocked()
	b.connectHole = b.holeEpoch
	b.connectAnnounced = false
	b.fetchSinceConnect = false
	holes := b.holeEpoch
	epoch := b.catchUpEpoch

	b.absorbLocked(StreamEvent{Kind: StreamConnected})

	sameHole := b.holeEpoch == holes
	sameEpoch := b.catchUpEpoch == epoch
	due := b.needCatchUp
	b.mu.Unlock()

	if !sameHole {
		t.Error("the hello raised a hole of its own; the read it belongs to would be thrown away for it")
	}
	if !sameEpoch {
		t.Error("the hello asked for another catch-up; the one it is announcing has not looked yet")
	}
	if !due {
		t.Error("the catch-up this connection asked for is no longer due")
	}
}

// A conversation the budget could not reach is read first next time. The walk
// used to take whatever the map yielded, so a busy conversation could be passed
// over on every pass while the budget went to quiet ones.
func TestASkippedConversationIsReadFirstNextTime(t *testing.T) {
	cursors := map[threadKey]string{
		{channel: "C1", threadTS: "1.000000"}: "",
		{channel: "C1", threadTS: "2.000000"}: "",
		{channel: "C2", threadTS: "3.000000"}: "",
	}
	waiting := map[threadKey]struct{}{
		{channel: "C2", threadTS: "3.000000"}: {},
	}

	order := threadWalkOrder(cursors, waiting)
	if len(order) != 3 {
		t.Fatalf("threadWalkOrder() = %+v, want every conversation", order)
	}
	if order[0].channel != "C2" {
		t.Errorf("threadWalkOrder() = %+v, want the one that waited last time first", order)
	}
	// And stable for the rest, so two passes with nothing waiting agree.
	if order[1].threadTS != "1.000000" || order[2].threadTS != "2.000000" {
		t.Errorf("threadWalkOrder() = %+v, want the rest in a settled order", order)
	}
}

// A reaction held for a catch-up is judged once more when that catch-up has
// run, and dropped quietly if it still belongs to no conversation of ours.
// Held for ever, these fill the queue that reports real losses.
func TestAHeldReactionIsJudgedOnceMoreAndThenDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	b.mu.Lock()
	b.needCatchUp = true
	b.absorbReactionLocked(Reaction{
		TS: "200.000100", Channel: otherChannel, User: colleague, Reaction: "eyes", Added: true,
	})
	b.mu.Unlock()

	if kept := b.drainReactions(generation); len(kept) != 0 {
		t.Fatalf("drainReactions() = %+v, want it held for the catch-up", kept)
	}
	b.mu.Lock()
	held := len(b.heldReactions)
	b.mu.Unlock()
	if held != 1 {
		t.Fatalf("held reactions = %d, want the one that matched nothing", held)
	}

	// The catch-up runs and explains nothing.
	b.mu.Lock()
	b.catchUpRuns++
	b.needCatchUp = false
	b.mu.Unlock()

	if kept := b.drainReactions(generation); len(kept) != 0 {
		t.Errorf("drainReactions() = %+v, want nothing: it belongs to no conversation of ours", kept)
	}
	b.mu.Lock()
	held = len(b.heldReactions)
	dropped := b.reactionsDropped
	b.mu.Unlock()
	if held != 0 {
		t.Errorf("held reactions = %d, want the wait to have ended", held)
	}
	if dropped {
		t.Error("dropping somebody else's emoji in somebody else's channel told the agent its count was wrong")
	}
}

// A connection that has been retired is not the live one, even if the
// replacement never opens. Cancelling does not stop a pump the instant it is
// called, and a Connect that fails leaves nothing to take over: between the
// two, the old pump would have gone on applying events as the live connection.
func TestAFailedReconnectRetiresTheConnectionItReplaced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	connector := &failingReconnector{api: &fakeAPI{botUserID: testBotUser}, stream: newFakeStream()}
	b := New(ctx, cfg, connector)
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	first := b.currentGeneration()

	// The connection drops and the reconnect fails.
	b.mu.Lock()
	b.connected = false
	b.mu.Unlock()
	connector.fail.Store(true)
	if _, err := b.Wait(ctx, 50*time.Millisecond); err == nil {
		t.Fatal("Wait() returned no error, want the failed reconnect reported")
	}

	if b.currentGeneration() == first {
		t.Error("the retired connection is still the live one; its pump can apply events and its catch-up can commit")
	}
}

// failingReconnector opens once and then refuses, which is what a reconnect
// against a workspace that has revoked the app looks like.
type failingReconnector struct {
	api    API
	stream *fakeStream
	fail   atomic.Bool
	once   sync.Once
}

func (c *failingReconnector) Connect(ctx context.Context, _ Config) (API, Stream, error) {
	if c.fail.Load() {
		return nil, nil, errors.New("cannot open the socket")
	}
	c.once.Do(func() {
		go func() {
			<-ctx.Done()
			c.stream.closeAll()
		}()
	})
	return c.api, c.stream, nil
}

// The wait for a closing socket is bounded, and the producer can still be
// running when it expires: it can put a reaction on a channel nobody will read
// again, and one that fits sets no marker of its own. The count is reported as
// short rather than left to be wrong quietly.
func TestAStreamThatWillNotCloseReportsItsReactionsAsLost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	// A connection being ended has almost always been replaced already, which
	// is the path this has to work on.
	stale := b.currentGeneration() - 1

	// Channels of this test's own: the events one never closes, and a reaction
	// is left on the other — which is what makes this a loss rather than a
	// slow close.
	events := make(chan StreamEvent)
	reactions := make(chan Reaction, 1)
	reactions <- Reaction{TS: "100.000500", Channel: testChannel, User: colleague, Reaction: "eyes", Added: true}
	b.finishStream(stale, stream, events, nil, reactions, nil)

	if !b.droppedReactionMark() {
		t.Error("the producer was still running with a reaction in hand when the wait for it expired, and nothing said the count might be short")
	}
}

// A close that is merely slow abandons nothing. Saying the count might be
// short every time the socket takes its time is how the one signal that means
// a vote went missing stops meaning anything.
func TestASlowCloseWithNothingLeftReportsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	stale := b.currentGeneration() - 1

	// The events channel never closes, and there is nothing left behind on the
	// others.
	events := make(chan StreamEvent)
	reactions := make(chan Reaction)
	b.finishStream(stale, stream, events, nil, reactions, nil)

	if b.droppedReactionMark() {
		t.Error("a slow close told the agent its count might be wrong with nothing missing")
	}
}

// An ordinary close says nothing. A marker that is set on every reconnect is a
// marker the agent learns to ignore.
func TestAStreamThatClosesInTimeReportsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	stale := b.currentGeneration() - 1

	events := make(chan StreamEvent)
	close(events)
	reactions := make(chan Reaction)
	b.finishStream(stale, stream, events, nil, reactions, nil)

	if b.droppedReactionMark() {
		t.Error("an ordinary close told the agent its count might be wrong")
	}
}

// A socket that has refused a message says so with an event on the very
// channel that had no room for one, so there is a moment where the hole exists
// and nothing has been asked for. A reaction judged in that moment is judged
// against the conversations a message nobody saw would have opened, so it
// waits a turn.
func TestAReactionWaitsOutTheMomentBeforeAnOverflowIsAnnounced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	// Nothing has been asked for yet: the refusal has happened on the socket
	// and the announcement has not arrived.
	b.mu.Lock()
	b.absorbReactionLocked(Reaction{
		TS: "200.000100", Channel: otherChannel, User: colleague, Reaction: "eyes", Added: true,
	})
	b.mu.Unlock()

	if kept := b.drainReactions(generation); len(kept) != 0 {
		t.Fatalf("drainReactions() = %+v, want it to wait for the announcement", kept)
	}

	// The announcement, and the mention it was hiding.
	b.mu.Lock()
	b.absorbLocked(StreamEvent{Kind: StreamDropped})
	b.absorbLocked(StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "200.000100", Channel: otherChannel, User: testOwner, Text: mention("ship it?"),
	}})
	b.mu.Unlock()

	if kept := b.drainReactions(generation); len(kept) != 1 {
		t.Errorf("drainReactions() = %+v, want the vote on the mention the overflow was hiding", kept)
	}
}

// A pass that hands the queue over in a storm leaves those messages in the
// window without moving the cursor. The pass that follows finds them in
// history and drops them as delivered — and if it took its cursor from what
// survived that, the cursor would stay behind them: the next restart would
// hand them over again, and every hole until then would fetch them again.
func TestTheCursorFollowsWhatAPassReadNotOnlyWhatItHandedOn(t *testing.T) {
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
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// The owner's message, handed over the way a storm hands it over: from the
	// queue, with no cursor moving.
	send(stream, testChannel, "100.000200", "", "hello?")
	eventually(t, "the pump to take it", func() bool { return b.pendingHomeCount() == 1 })
	b.mu.Lock()
	handed, _, err := b.handOverQueuesLocked(true)
	b.mu.Unlock()
	if err != nil || len(handed) != 1 {
		t.Fatalf("handOverQueuesLocked() = %v (err %v), want the queued message", texts(handed), err)
	}

	// History has it now, as it would by the time the storm passed.
	api.mu.Lock()
	api.channelHistory[testChannel] = append(api.channelHistory[testChannel], ownerMsg("100.000200", "hello?"))
	api.mu.Unlock()
	b.mu.Lock()
	b.needCatchUp = true
	b.mu.Unlock()

	if msgs := waitOnce(ctx, t, b).Messages; len(msgs) != 0 {
		t.Fatalf("Wait() = %v, want nothing: it was handed over in the storm", texts(msgs))
	}
	if got := b.Status().LastTS; got != "100.000200" {
		t.Errorf("last_ts = %q, want the message this pass read even though it handed none of it on", got)
	}

	// And it stays handed over across a restart, which is where a cursor left
	// behind would show.
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	eventuallyOnDisk(t, "the cursor to reach the state file", func() bool {
		stored, err := NewStore(cfg.StateDir).LastTS(testChannel)
		return err == nil && stored == "100.000200"
	})

	next := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = next.Close() })
	if msgs := waitOnce(ctx, t, next).Messages; len(msgs) != 0 {
		t.Errorf("Wait() after a restart = %v, want nothing: it was handed over before the session ended", texts(msgs))
	}
}

// A conversation the walk skipped for want of budget still has replies behind
// its cursor. A live reply in that conversation, delivered by the same pass,
// moves the cursor past them, and the walk that follows starts after them.
func TestASkippedConversationKeepsItsCursorWhenALiveReplyArrives(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatal(err)
	}
	cursors := map[threadKey]string{}
	for i := 0; i < maxThreadsPerCatchUp+1; i++ {
		ts := "50.00000" + strconv.Itoa(i)
		if err := store.SetThread("CPROJ", ts, "60.000000"); err != nil {
			t.Fatal(err)
		}
		cursors[threadKey{"CPROJ", ts}] = "60.000000"
	}
	order := threadWalkOrder(cursors, nil)
	target := order[len(order)-1] // the one a full walk skips

	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
		replies: []candidate{
			{Channel: "CPROJ", User: testOwner, Text: "unread one", TS: "70.000000", ThreadTS: target.threadTS},
			{Channel: "CPROJ", User: testOwner, Text: "unread two", TS: "80.000000", ThreadTS: target.threadTS},
		},
		historyGate: make(chan struct{}),
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	// A live reply in the skipped conversation, queued while the first
	// catch-up is held open on the home history.
	send(stream, "CPROJ", "90.000000", target.threadTS, "live")
	go func() {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if b.pendingThreadCount() == 1 {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		close(api.historyGate)
	}()

	result, err := b.Wait(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	t.Logf("first Wait delivered %v", texts(result.Messages))

	b.mu.Lock()
	cursor := b.threadCursors[target]
	skipped := b.threadsSkipped
	b.mu.Unlock()
	t.Logf("after the first pass: target cursor=%q threadsSkipped=%v", cursor, skipped)

	var got []string
	for i := 0; i < 3; i++ {
		r, err := b.Wait(ctx, 50*time.Millisecond)
		if err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		got = append(got, texts(r.Messages)...)
	}
	t.Logf("the walks after it delivered %v", got)
	api.mu.Lock()
	t.Logf("replies calls: %d", len(api.replyCalls))
	api.mu.Unlock()

	if cursor != "60.000000" {
		t.Errorf("the skipped conversation's cursor moved to %q on a live reply; the replies behind it were never read", cursor)
	}
	if len(got) != 2 {
		t.Errorf("replies in the skipped conversation delivered = %v, want [unread one unread two]", got)
	}
}

// The cursor a skipped conversation kept is moved by the walk that reads it,
// even when that walk hands nothing over. The ordinary shape of this is a
// conversation with nothing unread behind it: the owner replies, the reply is
// delivered from the queue, and the walk that follows finds only that reply and
// drops it as delivered. Left there, the cursor never moves — every later pass
// reads the conversation again, and a restart hands the reply over twice.
func TestAWalkThatDeliversNothingStillRecordsHowFarItRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatal(err)
	}
	cursors := map[threadKey]string{}
	for i := 0; i < maxThreadsPerCatchUp+1; i++ {
		ts := "50.00000" + strconv.Itoa(i)
		if err := store.SetThread("CPROJ", ts, "60.000000"); err != nil {
			t.Fatal(err)
		}
		cursors[threadKey{"CPROJ", ts}] = "60.000000"
	}
	order := threadWalkOrder(cursors, nil)
	target := order[len(order)-1] // the one a full walk skips

	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
		// Nothing unread behind the cursor: only the reply the socket is about
		// to deliver, which history has as well.
		replies: []candidate{
			{Channel: "CPROJ", User: testOwner, Text: "live", TS: "90.000000", ThreadTS: target.threadTS},
		},
		historyGate: make(chan struct{}),
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})

	send(stream, "CPROJ", "90.000000", target.threadTS, "live")
	go func() {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if b.pendingThreadCount() == 1 {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		close(api.historyGate)
	}()

	if result, err := b.Wait(ctx, 2*time.Second); err != nil {
		t.Fatalf("Wait() error = %v", err)
	} else if len(result.Messages) != 1 {
		t.Fatalf("Wait() = %v, want the live reply", texts(result.Messages))
	}

	b.mu.Lock()
	kept := b.threadCursors[target]
	b.mu.Unlock()
	if kept != "60.000000" {
		t.Fatalf("the skipped conversation's cursor = %q, want it left for the walk that reads it", kept)
	}

	// The walk that reads it. It hands nothing over — the only reply there has
	// been delivered already — and it still has to record how far it read.
	waitOnce(ctx, t, b)

	b.mu.Lock()
	moved := b.threadCursors[target]
	b.mu.Unlock()
	if moved != "90.000000" {
		t.Errorf("the cursor after the walk that read it = %q, want the reply it read", moved)
	}

	// Which is what a restart is decided by: the memory of what has been
	// handed over does not survive one.
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	next := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = next.Close() })
	for i := 0; i < 3; i++ {
		if msgs := waitOnce(ctx, t, next).Messages; len(msgs) != 0 {
			t.Fatalf("Wait() after a restart = %v, want nothing: it was handed over before the session ended", texts(msgs))
		}
	}
}

// A wait whose budget runs out while Slack is still thinking has read nothing
// — and what the connection delivered before it died is in the queues, already
// received. Reporting the closure over the top of it throws the owner's own
// messages away.
func TestAClosedConnectionHandsOverWhatItReceivedFirst(t *testing.T) {
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
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// The owner's message arrives, then the socket dies, and the catch-up a
	// reconnect asks for never gets an answer.
	send(stream, testChannel, "100.000200", "", "before it died")
	eventually(t, "the pump to take it", func() bool { return b.pendingHomeCount() == 1 })

	gate := make(chan struct{})
	defer close(gate)
	api.mu.Lock()
	api.historyGate = gate
	api.mu.Unlock()

	b.mu.Lock()
	b.needCatchUp = true
	b.mu.Unlock()
	stream.closeAll()

	result, err := b.Wait(ctx, 300*time.Millisecond)
	if err != nil {
		t.Fatalf("Wait() error = %v, want the message the connection had already delivered", err)
	}
	if len(result.Messages) != 1 || result.Messages[0].TS != "100.000200" {
		t.Errorf("Wait() = %v, want what was received before the closure", texts(result.Messages))
	}
}

// Connect returns before the socket is up, so a catch-up that runs in between
// reads a window that stops where it looked — and a message the owner sends
// before the socket starts relaying is in history and nowhere else. Socket Mode
// replays nothing, so the hello has to ask for the read that covers it.
func TestAMessageSentBeforeTheSocketCameUpIsStillRead(t *testing.T) {
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
	stream := newFakeStream()
	// The hello is held back, so it lands after the first catch-up has read
	// and committed — which is what a socket slower than a history request
	// looks like.
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream, quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	if b.catchUpDue() {
		t.Fatal("the first catch-up has not committed; this test is about what happens after it has")
	}

	// Sent while the socket was still coming up: history has it, and the
	// socket will never relay it.
	api.mu.Lock()
	api.channelHistory[testChannel] = append(api.channelHistory[testChannel], ownerMsg("100.000200", "sent before the socket was up"))
	api.mu.Unlock()

	b.absorb(StreamEvent{Kind: StreamConnected})

	if !b.catchUpDue() {
		t.Fatal("the hello asked for nothing; the window between the first read and the socket coming up is read by nobody else")
	}
	msgs := waitOnce(ctx, t, b).Messages
	if len(msgs) != 1 || msgs[0].TS != "100.000200" {
		t.Errorf("Wait() = %v, want the message sent before the socket came up", texts(msgs))
	}
}

// A sweep that spends its whole quota has not proved there is more to come: it
// may have taken the last message there was. Deferring the reactions on that
// assumption lets go of the lock with a pair split in two — the messages ready
// for whoever asks next, the reaction waiting for a turn with nothing to do.
func TestASweepThatEmptiesTheChannelAtItsBoundStillJudgesTheReactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// Exactly the room the sweep has, and nothing behind it.
	events := make(chan StreamEvent, 1)
	events <- StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "200.000100", Channel: otherChannel, User: testOwner, Text: mention("ship it?"),
	}}
	reactions := make(chan Reaction, 1)
	reactions <- Reaction{
		TS: "200.000100", Channel: otherChannel, User: colleague, Reaction: "white_check_mark", Added: true,
	}

	b.mu.Lock()
	applied, keep := b.takeReactionsLocked(events, reactions, nil, 1)
	b.mu.Unlock()

	if applied != 1 {
		t.Fatalf("takeReactionsLocked() applied %d messages, want the one that was there", applied)
	}
	if len(keep) != 0 {
		t.Fatalf("takeReactionsLocked() carried %+v, want it judged: the channel it was waiting behind is empty", keep)
	}
	if kept := b.drainReactions(b.currentGeneration()); len(kept) != 1 {
		t.Errorf("drainReactions() = %+v, want the vote with the mention it arrived with", kept)
	}
}

// A stream that closed after handing over exactly as many events as one sweep
// takes has closed cleanly. Calling that a timeout reports reactions lost that
// never were, on every connection that ends busy.
func TestAStreamThatClosedAtTheSweepBoundIsACleanClose(t *testing.T) {
	events := make(chan StreamEvent, maxSweep)
	for i := 0; i < maxSweep; i++ {
		events <- StreamEvent{Kind: StreamMessage, Message: Message{
			TS: fmt.Sprintf("500.%06d", i+1), Channel: testChannel, User: testOwner, Text: "busy",
		}}
	}
	close(events)

	late, closed := awaitStreamClose(events, nil)

	if len(late) != maxSweep {
		t.Errorf("awaitStreamClose() took %d events, want the sweep's worth", len(late))
	}
	if !closed {
		t.Error("awaitStreamClose() called a closed channel a timeout; every reaction on that connection is then reported lost")
	}
}

// The hello lands while the first catch-up is on Slack, which is the ordinary
// shape of it: Connect returns before the socket is up, and the read that
// follows goes out immediately. What that read found is still handed over —
// nothing has been missed from the stream, so there is no hole to place — and
// the window is read once more, for the stretch between the read and the
// socket coming up.
func TestAHelloDuringTheFirstReadAsksForOneMorePass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}
	api := &fakeAPI{
		botUserID: testBotUser,
		channelHistory: map[string][]candidate{testChannel: {
			ownerMsg("100.000100", "already answered"),
			ownerMsg("100.000200", "read by the first pass"),
		}},
		historyGate: make(chan struct{}),
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream, quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	done := make(chan WaitResult, 1)
	go func() {
		r, err := b.Wait(ctx, 2*time.Second)
		if err != nil {
			t.Errorf("Wait() error = %v", err)
		}
		done <- r
	}()
	eventually(t, "the first read to reach Slack", func() bool { return len(api.calls()) >= 1 })

	// The socket comes up while history is held open.
	b.absorb(StreamEvent{Kind: StreamConnected})
	b.mu.Lock()
	holes, due, sinceConnect := b.holeEpoch, b.needCatchUp, b.fetchSinceConnect
	b.mu.Unlock()
	t.Logf("after hello in flight: holeEpoch=%d needCatchUp=%v fetchSinceConnect=%v", holes, due, sinceConnect)

	close(api.historyGate)
	first := <-done
	t.Logf("first Wait: msgs=%v timed_out=%v history calls=%d", texts(first.Messages), first.TimedOut, len(api.calls()))
	if len(first.Messages) != 1 || first.Messages[0].TS != "100.000200" {
		t.Errorf("first Wait() = %v, want what the first pass read handed over (not discarded as a hole)", texts(first.Messages))
	}
	if !b.catchUpDue() {
		t.Error("after a hello in flight the window is not due for another read; the gap message would be left in history")
	}
	b.mu.Lock()
	holesAfter, cursor := b.holeEpoch, b.lastTS
	b.mu.Unlock()
	t.Logf("after first Wait: holeEpoch=%d cursor=%s", holesAfter, cursor)
	if holesAfter != holes {
		t.Errorf("holeEpoch moved %d -> %d; the hello in flight was treated as a hole", holes, holesAfter)
	}

	// Newer than the cursor the first pass committed: the pass the hello asked
	// for has to find it from where the cursor is.
	api.mu.Lock()
	api.channelHistory[testChannel] = append(api.channelHistory[testChannel], ownerMsg("100.000300", "sent in the gap"))
	api.mu.Unlock()

	second := waitOnce(ctx, t, b)
	t.Logf("second Wait: msgs=%v timed_out=%v history calls=%d", texts(second.Messages), second.TimedOut, len(api.calls()))
	if len(second.Messages) != 1 || second.Messages[0].TS != "100.000300" {
		t.Errorf("second Wait() = %v, want the message sent in the gap", texts(second.Messages))
	}
	if b.catchUpDue() {
		t.Error("a third pass is still due after the hello's pass; the flag never settles")
	}
}

// Probe (r7): with the fake connector's hello sent inside Connect, how often
// does the first session read history once (hello consumed) vs twice (hello
// after the read)? Informational: the real socket is slower than history.
func TestProbeHowOftenTheHelloBeatsTheFirstRead(t *testing.T) {
	once, twice, other := 0, 0, 0
	for i := 0; i < 30; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cfg := testConfig(t)
		cfg.IndicatorDisabled = true
		cfg.AutoAckDisabled = true
		if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
			t.Fatalf("seeding the cursor: %v", err)
		}
		api := &fakeAPI{
			botUserID:      testBotUser,
			channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
		}
		b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
		waitOnce(ctx, t, b)
		// Settle any second pass.
		waitOnce(ctx, t, b)
		n := 0
		for _, c := range api.calls() {
			if c.Channel == testChannel {
				n++
			}
		}
		switch n {
		case 1:
			once++
		case 2:
			twice++
		default:
			other++
		}
		_ = b.Close()
		cancel()
	}
	t.Logf("home-channel history reads on first connect over 30 sessions: once=%d twice=%d other=%d", once, twice, other)
}

// Conversations left unread are news for whoever is already waiting. A wait
// blocked on a long timeout has nothing in either queue to wake it, so without
// this the replies those conversations hold sit there until it gives up.
func TestAWalkThatRanOutOfBudgetWakesTheWaitingCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxThreadsPerCatchUp+1; i++ {
		if err := store.SetThread("CPROJ", "50.00000"+strconv.Itoa(i), "60.000000"); err != nil {
			t.Fatal(err)
		}
	}

	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream(), quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// Back to a catch-up that has not run, with nothing waiting in the queues:
	// the only news this pass can bring is the conversations it cannot reach.
	b.mu.Lock()
	b.needCatchUp = true
	b.threadsSkipped = false
	b.skippedThreads = nil
	b.mu.Unlock()

	woken := make(chan struct{})
	sub := b.subscribePending()
	defer b.unsubscribePending(sub)
	go func() {
		<-sub
		close(woken)
	}()

	if _, _, err := b.drainCatchUp(ctx, b.currentGeneration(), true, time.Second); err != nil {
		t.Fatalf("drainCatchUp() error = %v", err)
	}
	if !b.threadsWaiting() {
		t.Fatal("the walk reached every conversation; this test is about the ones it cannot")
	}

	select {
	case <-woken:
	case <-time.After(2 * time.Second):
		t.Error("nothing woke: a call already blocked hears about those conversations only when it gives up")
	}
}

// A connection that is still the live one, closing slowly with a reaction in
// its buffer: the reaction is delivered, not mourned. Saying the count might
// be short over something that was handed over is the false positive the
// marker cannot afford.
func TestASlowCloseDeliversTheReactionsItIsHolding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	// The events channel never closes, and a reaction is waiting on the other.
	events := make(chan StreamEvent)
	reactions := make(chan Reaction, 1)
	reactions <- Reaction{TS: "100.000500", Channel: testChannel, User: colleague, Reaction: "eyes", Added: true}

	b.finishStream(generation, stream, events, nil, reactions, nil)

	if kept := b.drainReactions(generation); len(kept) != 1 {
		t.Errorf("drainReactions() = %+v, want the reaction the closing connection was holding", kept)
	}
	if b.droppedReactionMark() {
		t.Error("the reaction was handed over and reported lost at the same time")
	}
}

// Clicks are the one thing no history can give back: a question whose answer
// is lost times out, and the buffer they wait in is the smallest of the three.
// The pump empties it every time round now rather than only under a flood of
// messages — a burst of reactions used to leave it untouched, because nothing
// counted the reactions and the sweep looked idle while it worked.
//
// This is a guard rather than a proof: the select picks between a ready click
// and a ready reaction at random, so a click already queued arrives either way.
// What the sweep protects against is the buffer filling while the pump works,
// which takes a real socket's volumes to show.
func TestABurstOfReactionsDoesNotHoldUpTheClicks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	done := make(chan AskResult, 1)
	go func() {
		result, err := b.Ask(ctx, AskRequest{
			Question:          "ship it?",
			Options:           []string{"yes", "no"},
			Timeout:           5 * time.Second,
			InterruptDisabled: true,
		})
		if err != nil {
			t.Errorf("Ask() error = %v", err)
		}
		done <- result
	}()
	waitForQuestion(b)

	// Other people's reactions, as fast as the socket could deliver them, and
	// the owner's click behind them.
	burst := make(chan struct{})
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		for i := 0; ; i++ {
			select {
			case <-burst:
				return
			case stream.reactions <- Reaction{
				TS: "100.000200", Channel: testChannel, User: colleague, Reaction: "eyes",
				Added: true, EventTS: fmt.Sprintf("%d", i),
			}:
			}
		}
	}()
	// Stopped before the test returns, so it cannot still be sending when the
	// cleanup closes the stream.
	defer func() {
		close(burst)
		<-burstDone
	}()

	stream.interactions <- click(testOwner, askTS, 0)

	select {
	case result := <-done:
		if result.ChoiceIndex != 0 {
			t.Errorf("Ask() = %+v, want the answer the owner clicked", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the click waited out the question while the pump worked through reactions")
	}
}

// The producer says when it has stopped, before it closes anything. Waiting on
// that rather than on a timer is what makes a reaction queued between the last
// send and the close something the bridge still takes.
func TestAStreamThatSaysItHasFinishedIsNotATimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}
	generation := b.currentGeneration()

	// Channels of this test's own. The events one never closes — what ends the
	// wait is the producer saying it has stopped — and a reaction is queued in
	// that last moment.
	events := make(chan StreamEvent)
	reactions := make(chan Reaction, 1)
	reactions <- Reaction{TS: "100.000500", Channel: testChannel, User: colleague, Reaction: "eyes", Added: true}
	stream.noteFinished()

	started := time.Now()
	b.finishStream(generation, stream, events, nil, reactions, nil)

	if took := time.Since(started); took >= streamCloseWait {
		t.Errorf("the teardown waited %v for a producer that had said it was done", took)
	}
	if kept := b.drainReactions(generation); len(kept) != 1 {
		t.Errorf("drainReactions() = %+v, want the reaction queued as the producer stopped", kept)
	}
	if b.droppedReactionMark() {
		t.Error("a stream that said it had finished was reported as a loss")
	}
}

// A page of somebody else's conversation has still been read. Leaving the
// cursor behind it means the next hole fetches the same page to learn the same
// thing — and with a busy channel that is every hole, for ever.
func TestTheCursorMovesPastAPageOfOtherPeoplesMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{
		botUserID: testBotUser,
		channelHistory: map[string][]candidate{testChannel: {
			ownerMsg("100.000100", "already answered"),
			// Said by somebody else: read, and not for the agent.
			{Channel: testChannel, User: colleague, Text: "not for us", TS: "100.000200"},
		}},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream(), quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout: nothing there is the owner's", result)
	}

	if got := b.Status().LastTS; got != "100.000200" {
		t.Errorf("last_ts = %q, want the newest message the read looked at", got)
	}
}
