package bridge

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
)

const askTS = "100.000700"

// askBridge returns a bridge ready to post a question, with the indicator wound
// down to test speed so the interplay between the two can be observed.
func askBridge(ctx context.Context, t *testing.T) (*Bridge, *fakeAPI, *fakeStream) {
	t.Helper()

	cfg := testConfig(t)
	cfg.IndicatorGrace = testGrace
	cfg.IndicatorInterval = testInterval
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{questionTS: askTS, postTS: "100.000900"}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })
	return b, api, stream
}

// click is the interaction Slack sends when the owner taps the option at index.
func click(user, messageTS string, index int) Interaction {
	return Interaction{
		User:      user,
		Channel:   testChannel,
		MessageTS: messageTS,
		BlockID:   askBlockID,
		ActionID:  askActionPrefix + string(rune('0'+index)),
		Value:     string(rune('0' + index)),
	}
}

func (f *fakeAPI) snapshotQuestions() []questionCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]questionCall(nil), f.questions...)
}

func (f *fakeAPI) snapshotResolutions() []updateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]updateCall(nil), f.resolutions...)
}

// The whole point of the tool: the owner taps an answer on their phone and the
// agent gets it back as an index it can branch on.
func TestAskReturnsTheClickedOption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		stream.interactions <- click(testOwner, askTS, 1)
	}()

	result, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No", "Later"}, MaxWaitTimeout, "")
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.TimedOut {
		t.Fatal("Ask() timed out, want the click returned")
	}
	if result.ChoiceIndex != 1 || result.ChoiceLabel != "No" {
		t.Errorf("Ask() = %+v, want index 1 labelled No", result)
	}
	if result.TS != askTS {
		t.Errorf("Ask() ts = %q, want the question's ts %q", result.TS, askTS)
	}

	questions := api.snapshotQuestions()
	if len(questions) != 1 {
		t.Fatalf("Ask() posted %d questions, want 1", len(questions))
	}
	q := questions[0].Question
	if len(q.Options) != 3 || q.Options[2].Value != "2" || q.Options[2].ActionID != askActionPrefix+"2" {
		t.Errorf("posted options = %+v, want one indexed button per choice", q.Options)
	}

	// The answered question must stop being clickable, or the owner can pick
	// twice and only the first answer will ever be heard.
	resolutions := api.snapshotResolutions()
	if len(resolutions) != 1 {
		t.Fatalf("Ask() resolved the question %d times, want 1", len(resolutions))
	}
	if !strings.Contains(resolutions[0].Text, "✅ No") || resolutions[0].TS != askTS {
		t.Errorf("resolution = %+v, want the chosen option shown on the question message", resolutions[0])
	}
}

// Only the owner's tap counts. Anyone else in the channel clicking must leave
// the agent waiting exactly as it was.
func TestAskIgnoresClicksThatAreNotTheOwnerAnsweringThisQuestion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		// Someone else in the channel, then a click on a different message —
		// a question from an earlier session, say, whose buttons are stale.
		stream.interactions <- click("U0INTRUDER", askTS, 0)
		stream.interactions <- click(testOwner, "100.000111", 0)
	}()

	result, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, 120*time.Millisecond, "")
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if !result.TimedOut {
		t.Fatalf("Ask() = %+v, want a timeout; neither click was the owner answering this question", result)
	}
	if result.ChoiceIndex != -1 {
		t.Errorf("Ask() choice_index = %d on a timeout, want -1 so it cannot be read as the first option", result.ChoiceIndex)
	}

	resolutions := api.snapshotResolutions()
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Text, "⌛") {
		t.Errorf("resolutions = %+v, want the expired question rewritten once", resolutions)
	}
}

// An unanswered question has to be retired too. Buttons nobody is listening for
// are worse than no buttons: the owner taps one and nothing happens.
func TestAskExpiresTheQuestionOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	result, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, 20*time.Millisecond, "")
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if !result.TimedOut || result.ChoiceLabel != "" {
		t.Fatalf("Ask() = %+v, want a bare timeout", result)
	}

	resolutions := api.snapshotResolutions()
	if len(resolutions) != 1 {
		t.Fatalf("Ask() resolved the question %d times, want 1", len(resolutions))
	}
	if !strings.Contains(resolutions[0].Text, "⌛ expired") || !strings.Contains(resolutions[0].Text, "Deploy now?") {
		t.Errorf("resolution text = %q, want the question marked expired", resolutions[0].Text)
	}
}

// Two questions at once would make a click ambiguous, so the second one is
// refused rather than queued behind the first.
func TestAskRefusesASecondQuestionWhileOneIsPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	first := make(chan error, 1)
	go func() {
		_, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, "")
		first <- err
	}()

	eventually(t, "the first question to be posted", func() bool {
		return len(b.pendingAskTS()) > 0
	})

	if _, err := b.Ask(ctx, "And now?", []string{"Yes", "No"}, MinWaitTimeout, ""); err == nil {
		t.Error("second Ask() = nil error, want it refused while a question is pending")
	}

	stream.interactions <- click(testOwner, askTS, 0)
	if err := <-first; err != nil {
		t.Fatalf("first Ask() error = %v", err)
	}

	// Once the first question is answered the slot is free again.
	go func() {
		time.Sleep(10 * time.Millisecond)
		stream.interactions <- click(testOwner, askTS, 1)
	}()
	if _, err := b.Ask(ctx, "And now?", []string{"Yes", "No"}, MaxWaitTimeout, ""); err != nil {
		t.Errorf("Ask() after the first was answered = %v, want it to succeed", err)
	}
}

// While the owner is deciding, the agent is not working — so the elapsed-time
// indicator has to stand down, and start again on the answer, which is new work
// exactly like a message from slack_wait.
func TestAskStopsTheIndicatorAndRestartsItOnTheAnswer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)
	api.mu.Lock()
	api.history = []candidate{
		ownerMsg("100.000100", "already answered"),
		ownerMsg("100.000200", "please look into this"),
	}
	api.mu.Unlock()

	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to appear", func() bool { return len(indicatorPosts(api)) == 1 })

	go func() {
		time.Sleep(2 * testGrace)
		stream.interactions <- click(testOwner, askTS, 0)
	}()

	if _, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, ""); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	// The first indicator was retired when the question went up: had it kept
	// running, its message would still be in the channel undeleted.
	eventually(t, "the first indicator to be deleted", func() bool { return len(api.snapshotDeletes()) == 1 })
	// And the answer starts the clock again.
	eventually(t, "a fresh indicator after the answer", func() bool { return len(indicatorPosts(api)) == 2 })
}

// A timed-out question is not new work, so nothing should start counting.
func TestAskLeavesTheIndicatorStoppedOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	if _, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, 20*time.Millisecond, ""); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	time.Sleep(4 * testGrace)
	if posts := indicatorPosts(api); len(posts) != 0 {
		t.Errorf("indicator posts = %+v after a timed-out question, want none", posts)
	}
}

// Messages arriving while the owner is being asked something must not be eaten
// by the ask; they are still owner input, and the next slack_wait owes them to
// the agent.
func TestAskKeepsMessagesForTheNextWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300", User: testOwner, Text: "one more thing"}}
		stream.interactions <- click(testOwner, askTS, 0)
	}()

	if _, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, ""); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 1 || result.Messages[0].Text != "one more thing" {
		t.Errorf("Wait() messages = %+v, want the message that arrived during the question", result.Messages)
	}
}

// Slack refuses a button label over 75 characters. A model writing a long
// option is a wording problem, not a reason to fail the owner's question.
func TestAskShortensLabelsToSlacksLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	long := strings.Repeat("a", 200)
	if _, err := b.Ask(ctx, "Which one?", []string{long, "short"}, 20*time.Millisecond, ""); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	questions := api.snapshotQuestions()
	if len(questions) != 1 {
		t.Fatalf("Ask() posted %d questions, want 1", len(questions))
	}
	label := questions[0].Question.Options[0].Label
	if len([]rune(label)) != maxOptionLabel {
		t.Errorf("label length = %d runes, want it cut to %d", len([]rune(label)), maxOptionLabel)
	}
	if !strings.HasSuffix(label, "…") {
		t.Errorf("label = %q, want an ellipsis marking what was cut", label)
	}
}

func TestAskRejectsUnusableOptionSets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	tests := map[string][]string{
		"no options":  {},
		"one option":  {"Yes"},
		"eleven":      make([]string, 11),
		"blank label": {"Yes", "   "},
	}
	for name, options := range tests {
		if _, err := b.Ask(ctx, "Which one?", options, MinWaitTimeout, ""); err == nil {
			t.Errorf("Ask() with %s = nil error, want it rejected", name)
		}
	}

	if _, err := b.Ask(ctx, "  ", []string{"Yes", "No"}, MinWaitTimeout, ""); err == nil {
		t.Error("Ask() with an empty question = nil error, want it rejected")
	}

	if posts := api.snapshotQuestions(); len(posts) != 0 {
		t.Errorf("a rejected question reached Slack: %+v", posts)
	}
}

// The bounds are Slack's button-per-block limit on one side and "this is not a
// question" on the other.
func TestAskAcceptsTheFullRangeOfOptionCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	options := make([]string, MaxAskOptions)
	for i := range options {
		options[i] = "option"
	}
	if _, err := b.Ask(ctx, "Which one?", options, 20*time.Millisecond, ""); err != nil {
		t.Fatalf("Ask() with %d options = %v, want it accepted", MaxAskOptions, err)
	}
	if got := len(api.snapshotQuestions()[0].Question.Options); got != MaxAskOptions {
		t.Errorf("posted %d buttons, want %d", got, MaxAskOptions)
	}
}

// An aborted call leaves nobody to receive an answer, so the buttons must go
// even though the tool call is on its way out with an error.
func TestAskExpiresTheQuestionWhenTheCallIsAborted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	callCtx, abort := context.WithCancel(ctx)
	go func() {
		time.Sleep(20 * time.Millisecond)
		abort()
	}()

	if _, err := b.Ask(callCtx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, ""); err == nil {
		t.Fatal("Ask() = nil error after the call was aborted, want the cancellation surfaced")
	}
	resolutions := api.snapshotResolutions()
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Text, "⌛") {
		t.Errorf("resolutions = %+v, want the abandoned question expired", resolutions)
	}
}

// If the question never reaches the channel, the agent is still working on
// what it was doing — and the owner should still be able to see that.
func TestAskRestoresTheIndicatorWhenTheQuestionCannotBePosted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.history = []candidate{
		ownerMsg("100.000100", "already answered"),
		ownerMsg("100.000200", "please look into this"),
	}
	api.questionErr = errors.New("channel_not_found")
	api.mu.Unlock()

	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to appear", func() bool { return len(indicatorPosts(api)) == 1 })

	if _, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, ""); err == nil {
		t.Fatal("Ask() = nil error when the question could not be posted, want the failure surfaced")
	}

	// The first indicator went down with the attempt; a new one has to take
	// its place, or the channel falls silent while the agent is still busy.
	eventually(t, "the indicator to come back", func() bool { return len(indicatorPosts(api)) == 2 })
}

// The owner can tap an answer before chat.postMessage has answered: Slack
// shows the buttons the moment the message exists. A click in that window is a
// real answer and must not be thrown away for arriving early.
func TestAskAcceptsAClickThatArrivesBeforeThePostReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	// A concurrent slack_wait reads the same interaction channel, so a click
	// can be routed while PostQuestion is still in flight — before the ask has
	// a timestamp to match it against. Routing it from inside the fake is that
	// race, made deterministic.
	api.mu.Lock()
	api.beforeQuestionReturns = func() { b.routeInteraction(click(testOwner, askTS, 1)) }
	api.mu.Unlock()

	result, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, "")
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.TimedOut || result.ChoiceIndex != 1 {
		t.Errorf("Ask() = %+v, want the early click honoured as choice 1", result)
	}
}

// select picks at random between a ready timer and a ready click, so a click
// already queued when the deadline fires must not be reported as a timeout.
func TestAskTakesAQueuedClickOverTheDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	// Queue the click first, then give the question a deadline that has
	// effectively already passed: both cases are ready at once.
	go func() {
		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })
		stream.interactions <- click(testOwner, askTS, 0)
	}()

	result, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MinWaitTimeout, "")
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.TimedOut {
		t.Errorf("Ask() = %+v, want the queued click honoured rather than a timeout", result)
	}
}

// A dead socket cannot deliver a click, so the buttons must not outlive it.
func TestAskExpiresTheQuestionWhenTheStreamCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)

	go func() {
		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })
		close(stream.events)
	}()

	if _, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, ""); err == nil {
		t.Fatal("Ask() = nil error after the connection closed, want the disconnection reported")
	}

	resolutions := api.snapshotResolutions()
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Text, "⌛") {
		t.Errorf("resolutions = %+v, want the unanswerable question expired", resolutions)
	}
}

// A click is in no history: if the queue it lands in is full, the owner's
// answer is gone. A backlog of messages must therefore not be able to fill it.
func TestAFullMessageQueueCannotSwallowAClick(t *testing.T) {
	stream := newTestStream(2)
	stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000100"}})
	stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000200"}})
	if !stream.dropped.Load() {
		// Guard the premise: the message queue has to be full for this to
		// mean anything.
		stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300"}})
	}

	callback := slack.InteractionCallback{
		Type: slack.InteractionTypeBlockActions,
		User: slack.User{ID: testOwner},
	}
	callback.Container.ChannelID = testChannel
	callback.Container.MessageTs = askTS
	callback.ActionCallback.BlockActions = []*slack.BlockAction{{
		BlockID:  askBlockID,
		ActionID: askActionPrefix + "0",
		Value:    "0",
	}}

	stream.handle(func(socketmode.Request) {}, socketmode.Event{
		Type:    socketmode.EventTypeInteractive,
		Data:    callback,
		Request: &socketmode.Request{},
	})

	select {
	case got := <-stream.interactions:
		if got.MessageTS != askTS {
			t.Errorf("interaction = %+v, want the click on %s", got, askTS)
		}
	default:
		t.Fatal("the click was dropped because the message queue was full; it cannot be recovered from history")
	}
}

// A closed channel is permanently ready. Without the closed check the ask
// loop would spin on it instead of reporting the disconnection, and the drain
// at the deadline would never finish at all.
func TestAskReportsTheDisconnectionWhenTheClickChannelCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)

	go func() {
		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })
		close(stream.interactions)
	}()

	done := make(chan error, 1)
	go func() {
		_, err := b.Ask(ctx, "Deploy now?", []string{"Yes", "No"}, MaxWaitTimeout, "")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Ask() = nil error after the click channel closed, want the disconnection reported")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Ask() never returned after the click channel closed")
	}

	resolutions := api.snapshotResolutions()
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Text, "⌛") {
		t.Errorf("resolutions = %+v, want the unanswerable question expired", resolutions)
	}
}

// pendingAskTS reports the ts of the question waiting for an answer, for tests
// that need to know the question has actually been posted.
func (b *Bridge) pendingAskTS() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ask == nil {
		return ""
	}
	return b.ask.ts
}

// Socket Mode redelivers any envelope it is not acknowledged for, so every
// interactive payload has to be acked — including the ones the bridge has no
// use for, which would otherwise come back forever.
func TestInteractiveEnvelopesAreAcknowledgedAndTranslated(t *testing.T) {
	stream := newTestStream(4)

	var acked int
	ack := func(socketmode.Request) { acked++ }

	callback := slack.InteractionCallback{
		Type: slack.InteractionTypeBlockActions,
		User: slack.User{ID: "U0INTRUDER"},
	}
	callback.Container.ChannelID = testChannel
	callback.Container.MessageTs = askTS
	callback.ActionCallback.BlockActions = []*slack.BlockAction{{
		BlockID:  askBlockID,
		ActionID: askActionPrefix + "1",
		Value:    "1",
	}}

	stream.handle(ack, socketmode.Event{
		Type:    socketmode.EventTypeInteractive,
		Data:    callback,
		Request: &socketmode.Request{},
	})

	// A payload the bridge cannot use at all still has to be acknowledged.
	stream.handle(ack, socketmode.Event{
		Type:    socketmode.EventTypeInteractive,
		Data:    slack.InteractionCallback{Type: slack.InteractionTypeViewSubmission},
		Request: &socketmode.Request{},
	})

	if acked != 2 {
		t.Errorf("acked %d interactive envelopes, want 2; Slack retries the rest", acked)
	}

	// Clicks arrive on their own channel: a click cannot be recovered from
	// history, so a queue full of messages must not be able to swallow one.
	if len(stream.events) != 0 {
		t.Errorf("the click was queued as a stream event; it belongs on the interaction channel")
	}
	got := <-stream.interactions
	want := Interaction{
		User:      "U0INTRUDER",
		Channel:   testChannel,
		MessageTS: askTS,
		BlockID:   askBlockID,
		ActionID:  askActionPrefix + "1",
		Value:     "1",
	}
	if got != want {
		t.Errorf("interaction = %+v, want %+v; the owner check belongs to the bridge, not the socket", got, want)
	}
	if len(stream.interactions) != 0 {
		t.Errorf("the unusable payload was queued as %d interaction(s), want 0", len(stream.interactions))
	}
}
