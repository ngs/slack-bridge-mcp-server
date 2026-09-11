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
	stream.events <- StreamEvent{Kind: StreamReaction, Reaction: Reaction{
		TS: ts, Channel: channel, User: user, Reaction: emoji, Added: added, EventTS: ts + "9",
	}}
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

	react(stream, otherChannel, "200.000100", colleague, "tada", true)
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
	if len(stream.events) != 2 {
		t.Fatalf("queued %d events, want both reactions", len(stream.events))
	}

	added := <-stream.events
	removed := <-stream.events
	want := StreamEvent{Kind: StreamReaction, Reaction: Reaction{
		TS:       "100.000500",
		Channel:  testChannel,
		User:     colleague,
		Reaction: "white_check_mark",
		Added:    true,
		EventTS:  "100.000600",
	}}
	if !reflect.DeepEqual(added, want) {
		t.Errorf("reaction_added = %+v, want %+v", added, want)
	}
	if removed.Kind != StreamReaction || removed.Reaction.Added {
		t.Errorf("reaction_removed = %+v, want a StreamReaction with added false", removed)
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

	if len(stream.events) != 0 {
		t.Errorf("queued %d events, want none for a reaction on a file", len(stream.events))
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
