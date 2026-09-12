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

	b.endStream(stale, b.currentStream(), nil, nil, nil, reactions, nil)

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
	b.endStream(stale, b.currentStream(), nil, nil, nil, nil, nil)

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

// A catch-up asked for again while this one was fetching means there is a hole
// after the window it read. The live messages queued behind that hole are newer
// than it, and handing them over would move the cursor past it — so that
// catch-up leaves them where they are, for the one that covers the hole.
func TestAGappedCatchUpKeepsTheLiveBacklog(t *testing.T) {
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
	for _, m := range got.msgs {
		if m.TS == "100.000300" {
			t.Fatal("the message behind the hole was handed over, which moves the cursor past what the hole swallowed")
		}
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

// The cursor has to move on an ordinary delivery. A guard written for the
// gapped case, where the queue is deliberately kept, would otherwise see the
// batch it is handing over still sitting in that queue and never advance —
// leaving every reconnect to replay what was just delivered.
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
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
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

	// Room for one of the two messages, so the sweep ends with the other still
	// on the channel.
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

	b.endStream(generation, b.currentStream(), nil, nil, nil, reactions, nil)

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

	// More than both sweeps of the closing path together, so the channel
	// cannot be emptied and the reactions have messages ahead of them.
	const sent = maxSweep + maxClosingSweep + 1
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

	b.endStream(generation, b.currentStream(), nil, events, nil, reactions, nil)

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
