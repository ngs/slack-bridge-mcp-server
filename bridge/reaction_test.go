package bridge

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/slack-go/slack/socketmode"
)

// react puts a reaction on the live stream the way the socket would, with no
// filtering applied: deciding whether it belongs to a conversation is the
// bridge's job, not the socket's.
func react(stream *fakeStream, channel, ts, user, emoji string, added bool) {
	stream.reactions <- Reaction{
		TS: ts, Channel: channel, User: user, Reaction: emoji, Added: added, EventTS: ts + "9",
	}
}

// waitOnce collects one delivery, reactions included.
func waitOnce(ctx context.Context, t *testing.T, b *Bridge) WaitResult {
	t.Helper()

	result, err := b.Wait(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return result
}

// The reason the feature exists: the owner posts something for people to vote
// on with emoji, and the session finds out that a vote was cast without
// polling reactions.get.
func TestAReactionInTheHomeChannelWakesAWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := mentionBridge(ctx, t)
	api.mu.Lock()
	api.names = map[string]string{colleague: "Sam Okada"}
	api.mu.Unlock()

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)

	result := waitOnce(ctx, t, b)
	if result.TimedOut {
		t.Fatal("Wait() timed out; a reaction has to end a wait the way a message does")
	}
	if len(result.Messages) != 0 {
		t.Errorf("Wait() returned %v, want no messages: a reaction is not one", texts(result.Messages))
	}
	if len(result.Reactions) != 1 {
		t.Fatalf("Wait() returned %d reactions, want 1", len(result.Reactions))
	}

	got := result.Reactions[0]
	want := Reaction{
		TS:       "100.000500",
		Channel:  testChannel,
		User:     colleague,
		UserName: "Sam Okada",
		Reaction: "white_check_mark",
		Added:    true,
		EventTS:  "100.0005009",
	}
	if got != want {
		t.Errorf("reaction = %+v, want %+v", got, want)
	}
}

// Taking an emoji off is an answer too — a vote withdrawn — so it is delivered
// as the same event with added false rather than dropped.
func TestAReactionRemovedIsDeliveredAsNotAdded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	react(stream, testChannel, "100.000500", testOwner, "eyes", false)

	result := waitOnce(ctx, t, b)
	if len(result.Reactions) != 1 {
		t.Fatalf("Wait() returned %d reactions, want 1", len(result.Reactions))
	}
	if result.Reactions[0].Added {
		t.Errorf("reaction = %+v, want added false for reaction_removed", result.Reactions[0])
	}
}

// A reaction relays whoever put it there. An approval is other people
// answering, so the owner-only rule that governs messages would make the whole
// feature useless.
func TestAReactionFromSomebodyElseIsDelivered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	react(stream, testChannel, "100.000500", colleague, "+1", true)

	result := waitOnce(ctx, t, b)
	if len(result.Reactions) != 1 || result.Reactions[0].User != colleague {
		t.Fatalf("Wait() returned %+v, want the colleague's reaction delivered", result.Reactions)
	}
	// Unresolved names fall back to the ID, exactly as slack_history does.
	if result.Reactions[0].UserName != colleague {
		t.Errorf("user_name = %q, want the raw ID when users.info cannot be reached", result.Reactions[0].UserName)
	}
}

// The scope of reactions is the scope of messages: the home channel and the
// channels where a conversation has been opened. A channel the app was merely
// added to is somebody else's workspace, and its emoji are not the session's
// business.
func TestAReactionInAnUntrackedChannelIsIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	react(stream, otherChannel, "200.000100", colleague, "tada", true)

	result := waitOnce(ctx, t, b)
	if !result.TimedOut || len(result.Reactions) != 0 {
		t.Fatalf("Wait() = %+v, want nothing from a channel with no conversation open", result)
	}

	// Once the owner opens a conversation there, the same channel's reactions
	// are part of it.
	send(stream, otherChannel, "200.000100", "", mention("take a look"))
	if msgs := waitOnce(ctx, t, b).Messages; len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the mention that opens the conversation", texts(msgs))
	}

	react(stream, otherChannel, "200.000100", colleague, "+1", true)
	result = waitOnce(ctx, t, b)
	if len(result.Reactions) != 1 || result.Reactions[0].Channel != otherChannel {
		t.Fatalf("Wait() returned %+v, want the reaction once the conversation is open", result.Reactions)
	}
}

// The bridge puts 👀 on everything it delivers. Relaying its own receipts would
// answer every message with an event about itself, and a session that reacts to
// what it reacted to has a loop in it.
func TestTheBridgesOwnReactionIsNotDelivered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	react(stream, testChannel, "100.000500", testBotUser, "eyes", true)

	result := waitOnce(ctx, t, b)
	if !result.TimedOut || len(result.Reactions) != 0 {
		t.Fatalf("Wait() = %+v, want the bridge's own receipt reaction dropped", result)
	}
}

// slack_ask blocks the loop that would otherwise collect live events, and it
// answers with a choice rather than with reactions. What arrives while a
// question is up therefore has to survive it and come back on the next wait,
// the way messages do.
func TestAReactionDuringAnAskIsDeliveredByTheNextWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	go func() {
		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })
		react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)
		stream.interactions <- click(testOwner, askTS, 0)
	}()

	answer, err := b.Ask(ctx, AskRequest{Question: "Ship it?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if answer.ChoiceIndex != 0 {
		t.Fatalf("Ask() = %+v, want the clicked option", answer)
	}

	result := waitOnce(ctx, t, b)
	if len(result.Reactions) != 1 || result.Reactions[0].Reaction != "white_check_mark" {
		t.Fatalf("Wait() returned %+v, want the reaction that arrived during the question", result.Reactions)
	}
}

// The Socket Mode envelope is where a reaction starts, so the translation from
// Slack's own event shape is worth pinning: an added and a removed event, both
// acknowledged, neither confused with a message.
func TestReactionEnvelopesAreTranslatedAndAcknowledged(t *testing.T) {
	stream := newTestStream(4)

	var acked int
	ack := func(socketmode.Request) { acked++ }

	stream.handle(ack, reactionEnvelope(t, "reaction_added", colleague, "white_check_mark"))
	stream.handle(ack, reactionEnvelope(t, "reaction_removed", colleague, "white_check_mark"))

	if acked != 2 {
		t.Errorf("acked %d reaction envelopes, want 2; Slack redelivers the rest", acked)
	}
	if len(stream.events) != 0 {
		t.Errorf("queued %d stream events, want 0: a reaction belongs on its own channel, where a message backlog cannot swallow it", len(stream.events))
	}
	if len(stream.reactions) != 2 {
		t.Fatalf("queued %d reactions, want both", len(stream.reactions))
	}

	added := <-stream.reactions
	removed := <-stream.reactions
	want := Reaction{
		TS:       "100.000500",
		Channel:  testChannel,
		User:     colleague,
		Reaction: "white_check_mark",
		Added:    true,
		EventTS:  "100.000600",
	}
	if !reflect.DeepEqual(added, want) {
		t.Errorf("reaction_added = %+v, want %+v", added, want)
	}
	if removed.Added {
		t.Errorf("reaction_removed = %+v, want added false", removed)
	}
}

// An emoji on a file has no message behind it: no channel to answer in, and no
// ts anything can be said about. Translating one would put an event the agent
// can do nothing with in front of it.
func TestAReactionOnAFileIsNotTranslated(t *testing.T) {
	stream := newTestStream(2)

	stream.handle(func(socketmode.Request) {}, eventsAPIEnvelope(t, `{
		"type": "event_callback",
		"event": {
			"type": "reaction_added",
			"user": "`+colleague+`",
			"reaction": "tada",
			"item": {"type": "file", "file": {"id": "F0THING"}},
			"event_ts": "100.000600"
		}
	}`))

	if len(stream.events) != 0 || len(stream.reactions) != 0 {
		t.Errorf("queued %d events and %d reactions, want none for a reaction on a file", len(stream.events), len(stream.reactions))
	}
}

// slack_reactions is the catch-up reactions have instead of history, so what it
// reports has to be the whole tally, named the way everything else names people.
func TestReactionsReportsTheTallyWithNames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := mentionBridge(ctx, t)
	api.mu.Lock()
	api.names = map[string]string{colleague: "Sam Okada"}
	api.messageReactions = []ReactionSummary{
		{Name: "white_check_mark", Count: 2, Users: []ReactionUser{{ID: colleague}, {ID: testOwner}}},
		{Name: "eyes", Count: 1, Users: []ReactionUser{{ID: testBotUser}}},
	}
	api.mu.Unlock()

	result, err := b.Reactions(ctx, ReactionsRequest{TS: "100.000500"})
	if err != nil {
		t.Fatalf("Reactions() error = %v", err)
	}

	want := []ReactionSummary{
		{Name: "white_check_mark", Count: 2, Users: []ReactionUser{
			{ID: colleague, UserName: "Sam Okada"},
			{ID: testOwner, UserName: testOwner},
		}},
		{Name: "eyes", Count: 1, Users: []ReactionUser{{ID: testBotUser, UserName: testBotUser}}},
	}
	if !reflect.DeepEqual(result.Reactions, want) {
		t.Errorf("Reactions() = %+v, want %+v", result.Reactions, want)
	}

	api.mu.Lock()
	reads := append([]reactionCall(nil), api.reactionReads...)
	api.mu.Unlock()
	if len(reads) != 1 || reads[0].Channel != testChannel || reads[0].TS != "100.000500" {
		t.Errorf("reactions.get calls = %+v, want one against the home channel", reads)
	}
}

// reactionEnvelope builds the Socket Mode envelope Slack sends for a reaction
// on a message.
func reactionEnvelope(t *testing.T, eventType, user, emoji string) socketmode.Event {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"type": "event_callback",
		"event": map[string]any{
			"type":     eventType,
			"user":     user,
			"reaction": emoji,
			"item": map[string]any{
				"type":    "message",
				"channel": testChannel,
				"ts":      "100.000500",
			},
			"item_user": testOwner,
			"event_ts":  "100.000600",
		},
	})
	if err != nil {
		t.Fatalf("building the reaction payload: %v", err)
	}
	return eventsAPIEnvelope(t, string(payload))
}

// A ts is the whole of what identifies a message, so an empty one is answered
// here rather than by Slack: the deterministic error is the one the caller can
// act on, and it costs no connection.
func TestReactionsRequiresATimestamp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := mentionBridge(ctx, t)

	if _, err := b.Reactions(ctx, ReactionsRequest{}); err == nil {
		t.Fatal("Reactions() = nil error for an empty ts, want it refused")
	}

	api.mu.Lock()
	reads := len(api.reactionReads)
	api.mu.Unlock()
	if reads != 0 {
		t.Errorf("made %d reactions.get calls, want none: the request never reaches Slack", reads)
	}
}

// A reaction is in no history, so one that cannot be queued is a vote nobody
// ever counts. A backlog of messages — the one thing that fills the event
// queue — must therefore not be able to take the space a reaction needs.
func TestAFullMessageQueueCannotSwallowAReaction(t *testing.T) {
	stream := newTestStream(2)
	stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000100"}})
	stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000200"}})
	if !stream.dropped.Load() {
		// Guard the premise: the message queue has to be full for this to
		// mean anything.
		stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300"}})
	}

	stream.handle(func(socketmode.Request) {}, reactionEnvelope(t, "reaction_added", colleague, "white_check_mark"))

	select {
	case got := <-stream.reactions:
		if got.TS != "100.000500" {
			t.Errorf("reaction = %+v, want the one on 100.000500", got)
		}
	default:
		t.Fatal("the reaction was dropped because the message queue was full; no history call brings it back")
	}
}

// Stream and API are exported, so a connector written before reactions existed
// has neither half. It has to keep working — delivering no reactions, and
// saying plainly that the tally cannot be read.
func TestAConnectorWithoutTheReactionHalvesStillWorks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	b := New(ctx, cfg, olderConnector{api: &olderAPI{fakeAPI: &fakeAPI{}}, stream: &olderStream{fakeStream: newFakeStream()}})
	t.Cleanup(func() { _ = b.Close() })

	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a plain timeout from a stream with no reactions", result)
	}

	_, err := b.Reactions(ctx, ReactionsRequest{TS: "100.000500"})
	if err == nil {
		t.Fatal("Reactions() = nil error on a connection that cannot read them, want it said plainly")
	}
}

// olderAPI and olderStream are an API and a Stream as they were before
// reactions. Each shadows the optional method with one of a different shape, so
// the embedded fake's version cannot satisfy the interface through them — which
// is what a type written against the old interfaces looks like from here.
type olderAPI struct{ *fakeAPI }

func (a *olderAPI) MessageReactions() {}

type olderStream struct{ *fakeStream }

func (s *olderStream) Reactions() {}

// olderConnector hands out that pair, which fakeConnector cannot: its fields
// are the concrete fakes.
type olderConnector struct {
	api    API
	stream Stream
}

func (c olderConnector) Connect(context.Context, Config) (API, Stream, error) {
	// Deliberately not closed with its connection: this stands in for a
	// connector written before any of this, and the pump's wait for the close
	// is bounded precisely so one like it cannot hold shutdown up.
	return c.api, c.stream, nil
}

// Messages and reactions arrive on channels of their own, and a select picks
// between two ready channels at random. A reaction on the very message that
// opens a conversation must not be judged before that message has opened it:
// Socket Mode delivered the mention first, and classifying the reaction ahead
// of it would drop a vote that was never anybody's to lose.
//
// The race is a coin flip per attempt, so the scenario is run enough times that
// the old ordering could not have survived it.
func TestAReactionOnTheOpeningMentionIsNotDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for attempt := 0; attempt < 10; attempt++ {
		b, _, stream := mentionBridge(ctx, t)

		// Both are already on the wire when the wait starts, which is what a
		// colleague reacting the moment the owner asks looks like.
		send(stream, otherChannel, "200.000100", "", mention("ship it?"))
		react(stream, otherChannel, "200.000100", colleague, "white_check_mark", true)

		// The message and the reaction need not come back in the same
		// delivery, only both. The deadline is generous because a timeout
		// here would mean the wait was slower than the clock, not that
		// anything was dropped.
		messages, reactions := collectBoth(ctx, t, b)

		if len(messages) != 1 {
			t.Fatalf("attempt %d: delivered %v, want the mention", attempt, texts(messages))
		}
		if len(reactions) != 1 {
			t.Fatalf("attempt %d: delivered %d reactions, want the vote on the message that opened the conversation", attempt, len(reactions))
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
}

// collectBoth waits until at least one message and one reaction have been
// handed over, or until a wait comes back empty. Each wait returns the moment
// something is there, so the generous deadline costs nothing except when
// something really is missing.
func collectBoth(ctx context.Context, t *testing.T, b *Bridge) ([]Message, []Reaction) {
	t.Helper()

	var (
		messages  []Message
		reactions []Reaction
	)
	for i := 0; i < 3; i++ {
		result, err := b.Wait(ctx, 2*time.Second)
		if err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		messages = append(messages, result.Messages...)
		reactions = append(reactions, result.Reactions...)
		if result.TimedOut || (len(messages) > 0 && len(reactions) > 0) {
			break
		}
	}
	return messages, reactions
}

// The bridge puts a receipt on every message it delivers, and a catch-up can
// deliver hundreds at once. Those echoes are dropped at the socket rather than
// at the delivery, so they never take the space a colleague's vote needs.
func TestTheBridgesOwnReceiptsNeverEnterTheQueue(t *testing.T) {
	stream := newTestStream(4)
	stream.botUserID = testBotUser

	stream.handle(func(socketmode.Request) {}, reactionEnvelope(t, "reaction_added", testBotUser, "eyes"))
	if len(stream.reactions) != 0 {
		t.Errorf("queued %d reactions, want 0: the bridge's own receipt is its own echo", len(stream.reactions))
	}

	// Somebody else's reaction still gets through.
	stream.handle(func(socketmode.Request) {}, reactionEnvelope(t, "reaction_added", colleague, "white_check_mark"))
	if len(stream.reactions) != 1 {
		t.Errorf("queued %d reactions, want the colleague's", len(stream.reactions))
	}
}

// Reactions that reached the bridge before the socket died are not the loss the
// live-only limitation describes: they were received. Whether the wait notices
// the reaction or the closed events channel first is a coin flip, so the
// promise is the one that holds either way — the reaction is delivered, or it
// is kept for the next call. What it must never be is gone.
func TestBufferedReactionsSurviveTheStreamClosing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for attempt := 0; attempt < 10; attempt++ {
		b, _, stream := mentionBridge(ctx, t)

		react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)
		stream.closeEvents()

		// Generous, because both of the outcomes below are reached at once:
		// a short deadline could fire first on a slow machine and say nothing
		// about whether the reaction survived.
		result, err := b.Wait(ctx, 5*time.Second)
		switch {
		case err == nil && len(result.Reactions) == 1:
			// Delivered before the closure was noticed.
		case err != nil && b.pendingReactionCount() == 1:
			// The closure was noticed first, and the reaction was kept.
		default:
			t.Fatalf("attempt %d: Wait() = %+v, err = %v, pending = %d; the reaction that arrived before the socket died was lost",
				attempt, result, err, b.pendingReactionCount())
		}
		if err := b.Close(); err != nil {
			t.Fatalf("attempt %d: Close() error = %v", attempt, err)
		}
	}
}

// pendingReactionCount reports how many reactions are waiting to be handed
// over, for tests that need to see one survive a call that failed.
func (b *Bridge) pendingReactionCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pendingReactions)
}

// A wait that has both hands over both. Messages and reactions sit on channels
// of their own, so without a sweep before the batch is decided, a message ready
// at the same instant as a reaction goes back alone — and the agent counts a
// vote one call after it was already sent.
func TestAMessageAndAReactionArriveInOneDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	send(stream, testChannel, "100.000200", "", "ship it?")
	react(stream, testChannel, "100.000200", colleague, "white_check_mark", true)

	result, err := b.Wait(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 1 || len(result.Reactions) != 1 {
		t.Fatalf("Wait() returned %d messages and %d reactions, want one delivery carrying both",
			len(result.Messages), len(result.Reactions))
	}
}

// Slack redelivers any envelope it is not acknowledged for, and the
// acknowledgement can fail. A message survives that because history merges by
// timestamp; a reaction has no history and no merge, so the same vote would be
// counted twice unless it is remembered.
//
// The memory is on the bridge rather than on the stream deliberately: a
// reconnect replaces the stream, the redelivery can arrive on the replacement,
// and a window that was replaced along with the connection would be empty
// exactly when it was needed.
func TestARedeliveredReactionIsDeliveredOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)
	if result := waitOnce(ctx, t, b); len(result.Reactions) != 1 {
		t.Fatalf("Wait() returned %+v, want the vote", result.Reactions)
	}

	// The same event again, as a redelivery carries it, including its event_ts.
	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)
	if result := waitOnce(ctx, t, b); len(result.Reactions) != 0 {
		t.Errorf("Wait() returned %+v, want nothing: the redelivery is the vote already counted", result.Reactions)
	}

	// The same person taking the emoji off is a different action, and has to
	// get through.
	react(stream, testChannel, "100.000500", colleague, "white_check_mark", false)
	if result := waitOnce(ctx, t, b); len(result.Reactions) != 1 || result.Reactions[0].Added {
		t.Errorf("Wait() returned %+v, want the removal delivered", result.Reactions)
	}
}

// A reaction that did not fit in the queue is gone, and no history brings it
// back. The one honest thing left is to say so, so the agent knows its count is
// wrong and reads the tally back.
func TestADroppedReactionIsReportedToTheAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	stream.reactionsDropped.Store(true)

	// Nothing else to hand over, so the loss is the whole of what there is to
	// say — and it is said straight away rather than at the end of a poll the
	// agent is spending on a count it cannot know is wrong.
	result := waitOnce(ctx, t, b)
	if !result.ReactionsDropped {
		t.Fatalf("Wait() = %+v, want the loss reported", result)
	}
	if result.TimedOut {
		t.Error("Wait() timed out with a loss to report, want it handed over as a delivery")
	}

	// And it is reported once: the record is cleared by the call that carried
	// it, so the next wait is not still complaining about an old loss.
	if result := waitOnce(ctx, t, b); result.ReactionsDropped {
		t.Error("reactions_dropped = true on the next wait as well; the loss was already reported")
	}
}

// A reaction is judged when the batch goes out, not when it arrives. The
// mention that brings the agent into a channel can still be on the socket, held
// by another call, or waiting in history to be recovered on a reconnect — and a
// vote must not be lost to whichever of those it is.
func TestAReactionIsJudgedAgainstTheScopeAtDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the home cursor: %v", err)
	}
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatalf("seeding the mention cursor: %v", err)
	}

	// The mention is not on the socket at all: it was sent while the session
	// was down, and only the search through history will find it.
	api := &fakeAPI{
		botUserID: testBotUser,
		joined:    []string{otherChannel, testChannel},
		channelHistory: map[string][]candidate{
			testChannel:  {ownerMsg("100.000100", "already answered")},
			otherChannel: {{Channel: otherChannel, User: testOwner, Text: mention("ship it?"), TS: "300.000300"}},
		},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	// The vote lands live, before catch-up has opened the conversation it
	// belongs to.
	react(stream, otherChannel, "300.000300", colleague, "white_check_mark", true)

	messages, reactions := collectBoth(ctx, t, b)
	if len(messages) != 1 {
		t.Fatalf("delivered %v, want the mention", texts(messages))
	}
	if len(reactions) != 1 {
		t.Fatalf("delivered %d reactions, want the vote that arrived before catch-up opened the conversation", len(reactions))
	}
}

// Draining a dead connection must also record that it is dead. Otherwise the
// call hands its batch over as a success, the bridge still believes it is
// connected, and the next slack_wait listens to a closed socket and reports a
// disconnection that has already been dealt with.
func TestDrainingAClosedStreamMarksItDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)
	stream.closeEvents()

	_, _ = b.Wait(ctx, 5*time.Second)

	if b.Status().Connected {
		t.Error("status reports connected after the stream closed; the next wait would listen to a dead socket")
	}
}

// A loss recorded on a connection that then died is still a loss the agent has
// to hear about, and the stream it happened on is gone.
func TestADroppedReactionSurvivesTheConnectionItWasLostOn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	stream.reactionsDropped.Store(true)
	stream.closeEvents()

	// Whichever the call notices first — the loss or the closure — the loss is
	// not what goes missing: either it comes back with this result, or it is
	// still on the bridge for the next call.
	result, err := b.Wait(ctx, 5*time.Second)
	if err == nil && result.ReactionsDropped {
		return
	}
	if !b.droppedReactionMark() {
		t.Error("the record of a lost reaction died with the connection; the agent would never learn its count is wrong")
	}
}

// droppedReactionMark reports whether the bridge is still holding a loss to
// report, for tests that need to see one outlive its connection.
func (b *Bridge) droppedReactionMark() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reactionsDropped
}

// A timestamp identifies a message only within its channel, so an item carrying
// one without the other is not addressable. Accepting it would report the
// reaction in the home channel, which is simply the wrong place.
func TestAReactionWithNoChannelIsNotTranslated(t *testing.T) {
	stream := newTestStream(2)

	stream.handle(func(socketmode.Request) {}, eventsAPIEnvelope(t, `{
		"type": "event_callback",
		"event": {
			"type": "reaction_added",
			"user": "`+colleague+`",
			"reaction": "tada",
			"item": {"type": "message", "ts": "100.000500"},
			"event_ts": "100.000600"
		}
	}`))

	if len(stream.reactions) != 0 {
		t.Errorf("queued %d reactions, want none for an item with no channel", len(stream.reactions))
	}
}

// The socket closes its channels together and a call can notice any of them
// first. Whichever it is, the record of a lost reaction has to come off the
// dying stream: it is the agent's only sign that the count it is keeping is
// wrong, and there is no history to recover it from.
func TestADroppedReactionIsRescuedWhicheverChannelClosesFirst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	// The click channel goes first, which is what the close order actually
	// does: the events channel is the one a disconnection is reported from,
	// and it is closed last.
	stream.reactionsDropped.Store(true)
	stream.closeInteractions()

	result, err := b.Wait(ctx, 5*time.Second)
	if err == nil && !result.ReactionsDropped {
		t.Fatal("Wait() = nil error and no loss after the click channel closed, want one or the other")
	}
	if err != nil && !b.droppedReactionMark() {
		t.Error("the loss died with the connection because a different channel closed first")
	}
}

// A delivery carrying only reactions still reports an empty message array. A
// caller reading the bridge directly has always had one, and null is a
// different shape to parse.
func TestAReactionOnlyDeliveryStillCarriesAnEmptyMessageArray(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)

	result := waitOnce(ctx, t, b)
	if len(result.Reactions) != 1 {
		t.Fatalf("Wait() returned %+v, want the reaction", result)
	}
	if result.Messages == nil {
		t.Error("messages is null on a reaction-only delivery, want the empty array every other result carries")
	}
}

// A call that gives up its turn at catch-up still hands over what the socket
// already delivered, and a reaction is delivered on its own: nobody has to
// have said anything for an emoji to be news. A hand-over that only looked at
// the message queues would leave it there, and the call that was told "nothing
// yet" was holding the answer all along.
//
// Fail-first: with the early return looking only at the two message queues,
// the drain below comes back empty.
func TestAHandOverWithoutReadingStillTakesTheReactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)
	eventually(t, "the reaction to reach the queue", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.reactionsWaitingLocked()
	})

	// Somebody else's request is out and this call cannot wait for it. The
	// message queues are empty; the emoji queue is not.
	b.catchUpSlot <- struct{}{}
	defer func() { <-b.catchUpSlot }()

	msgs, reactions, err := b.drainCatchUp(ctx, b.currentGeneration(), true, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("drainCatchUp() error = %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("messages = %+v, want none; nothing was said", msgs)
	}
	if len(reactions) != 1 {
		t.Fatalf("reactions = %+v, want the one the socket delivered; a wait that gave up its turn sits out its whole timeout with the answer already in the bridge", reactions)
	}
}
