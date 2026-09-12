package bridge

import (
	"context"
	"errors"
	"fmt"
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

// waitForQuestion blocks until a question is up. It polls rather than using
// eventually because it runs in a goroutine, and the test's own Fatalf belongs
// to the test's goroutine; a question that never appears is caught by the test
// timeout instead.
func waitForQuestion(b *Bridge) {
	for b.pendingAskTS() == "" {
		time.Sleep(2 * time.Millisecond)
	}
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
		// Once the question exists, not merely once some time has passed: a
		// click with no question pending is answering nothing, and the pump
		// that owns the socket says so straight away rather than leaving it on
		// the channel for whoever asks next.
		waitForQuestion(b)
		stream.interactions <- click(testOwner, askTS, 1)
	}()

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No", "Later"}, Timeout: MaxWaitTimeout, ThreadTS: ""})
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
	// The label is quoted literally: it was chosen on a plain_text button, and
	// the resolved question is Markdown.
	if !strings.Contains(resolutions[0].Text, "✅ `No`") || resolutions[0].TS != askTS {
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
		waitForQuestion(b)
		// Someone else in the channel, then a click on a different message —
		// a question from an earlier session, say, whose buttons are stale.
		stream.interactions <- click("U0INTRUDER", askTS, 0)
		stream.interactions <- click(testOwner, "100.000111", 0)
	}()

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: 120 * time.Millisecond, ThreadTS: ""})
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

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: 20 * time.Millisecond, ThreadTS: ""})
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
		_, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""})
		first <- err
	}()

	eventually(t, "the first question to be posted", func() bool {
		return len(b.pendingAskTS()) > 0
	})

	if _, err := b.Ask(ctx, AskRequest{Question: "And now?", Options: []string{"Yes", "No"}, Timeout: MinWaitTimeout, ThreadTS: ""}); err == nil {
		t.Error("second Ask() = nil error, want it refused while a question is pending")
	}

	stream.interactions <- click(testOwner, askTS, 0)
	if err := <-first; err != nil {
		t.Fatalf("first Ask() error = %v", err)
	}

	// Once the first question is answered the slot is free again.
	go func() {
		waitForQuestion(b)
		stream.interactions <- click(testOwner, askTS, 1)
	}()
	if _, err := b.Ask(ctx, AskRequest{Question: "And now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""}); err != nil {
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

	if _, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	// The first indicator was retired when the question went up: had it kept
	// running, its message would still be in the channel undeleted.
	eventually(t, "the first indicator to be deleted", func() bool { return len(api.snapshotDeletes()) == 1 })
	// And the answer starts the clock again.
	eventually(t, "a fresh indicator after the answer", func() bool { return len(indicatorPosts(api)) == 2 })
}

// A question asked inside a thread is answered there, and the work that
// follows the answer belongs in the same place: the owner tapped a button in
// the thread and is watching that thread for what happens next.
func TestAskInAThreadRestartsTheIndicatorInThatThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)

	go func() {
		waitForQuestion(b)
		stream.interactions <- click(testOwner, askTS, 0)
	}()

	if _, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: "100.000200"}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	eventually(t, "an indicator after the answer", func() bool { return len(indicatorPosts(api)) == 1 })
	if got := indicatorPosts(api)[0]; got.ThreadTS != "100.000200" {
		t.Errorf("indicator posted as %+v, want it in thread 100.000200 where the question was asked", got)
	}
}

// A timed-out question is not new work, so nothing should start counting.
func TestAskLeavesTheIndicatorStoppedOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	if _, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: 20 * time.Millisecond, ThreadTS: ""}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	time.Sleep(4 * testGrace)
	if posts := indicatorPosts(api); len(posts) != 0 {
		t.Errorf("indicator posts = %+v after a timed-out question, want none", posts)
	}
}

// Slack refuses a button label over 75 characters. A model writing a long
// option is a wording problem, not a reason to fail the owner's question.
func TestAskShortensLabelsToSlacksLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	long := strings.Repeat("a", 200)
	if _, err := b.Ask(ctx, AskRequest{Question: "Which one?", Options: []string{long, "short"}, Timeout: 20 * time.Millisecond, ThreadTS: ""}); err != nil {
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
		if _, err := b.Ask(ctx, AskRequest{Question: "Which one?", Options: options, Timeout: MinWaitTimeout, ThreadTS: ""}); err == nil {
			t.Errorf("Ask() with %s = nil error, want it rejected", name)
		}
	}

	if _, err := b.Ask(ctx, AskRequest{Question: "  ", Options: []string{"Yes", "No"}, Timeout: MinWaitTimeout, ThreadTS: ""}); err == nil {
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
	if _, err := b.Ask(ctx, AskRequest{Question: "Which one?", Options: options, Timeout: 20 * time.Millisecond, ThreadTS: ""}); err != nil {
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

	if _, err := b.Ask(callCtx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""}); err == nil {
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

	if _, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""}); err == nil {
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

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""})
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
		waitForQuestion(b)
		stream.interactions <- click(testOwner, askTS, 0)
	}()

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MinWaitTimeout, ThreadTS: ""})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.TimedOut {
		t.Errorf("Ask() = %+v, want the queued click honoured rather than a timeout", result)
	}
}

// A ts identifies a message only inside its channel, and the buffer that holds
// clicks during the posting window must not be fillable by traffic that could
// never be this question's answer.
func TestAskChecksWhoAndWhereBeforeBufferingAClick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	api.mu.Lock()
	api.beforeQuestionReturns = func() {
		// A click from elsewhere, one from someone else, and one on a
		// different block — none of them can be the answer, and together they
		// would fill the buffer if they were let in.
		for range maxEarlyClicks {
			elsewhere := click(testOwner, askTS, 0)
			elsewhere.Channel = "C0OTHER"
			b.routeInteraction(elsewhere)

			stranger := click("U0INTRUDER", askTS, 0)
			b.routeInteraction(stranger)

			otherBlock := click(testOwner, askTS, 0)
			otherBlock.BlockID = "some_other_block"
			b.routeInteraction(otherBlock)
		}
		// And then the real one.
		b.routeInteraction(click(testOwner, askTS, 1))
	}
	api.mu.Unlock()

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.TimedOut || result.ChoiceIndex != 1 {
		t.Errorf("Ask() = %+v, want the owner's own click honoured despite the noise", result)
	}
}

// A click can be taken off the channel by another goroutine and be part-way
// to the question when the deadline fires. Expiring on the spot would throw
// away an answer the owner had already given.
func TestAskWaitsForAClickThatIsAlreadyOnItsWay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)

	go func() {
		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })
		// Delivered directly, the way a concurrent reader that had already
		// taken the click off the channel would: it is in neither queue when
		// the timer fires. The sleep puts it just past the deadline, inside
		// the settling window.
		time.Sleep(60 * time.Millisecond)
		b.routeInteraction(click(testOwner, askTS, 1))
	}()

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: 40 * time.Millisecond, ThreadTS: ""})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.TimedOut || result.ChoiceIndex != 1 {
		t.Errorf("Ask() = %+v, want the in-flight click honoured at the deadline", result)
	}
}

// A dead socket cannot deliver a click, so the buttons must not outlive it.
func TestAskExpiresTheQuestionWhenTheStreamCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)

	go func() {
		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })
		stream.closeEvents()
	}()

	if _, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""}); err == nil {
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
		stream.closeInteractions()
	}()

	done := make(chan error, 1)
	go func() {
		_, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout, ThreadTS: ""})
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

// The timeout a question is given is a promise about when the tool returns,
// and the backlog it collects on the way out is made of Slack requests. One
// that hangs used to hold the call open indefinitely: the wait for a turn at
// catch-up was bounded and the request itself was not.
func TestAQuestionAnswersOnTimeEvenWithSlackNotAnswering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)

	// Connect first, so the question is asked on a live connection.
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// Slack stops answering, and a catch-up is due — so the question's backlog
	// collection has somewhere to hang.
	gate := make(chan struct{})
	defer close(gate)
	api.mu.Lock()
	api.historyGate = gate
	api.mu.Unlock()

	b.mu.Lock()
	b.needCatchUp = true
	b.mu.Unlock()

	type answered struct {
		result AskResult
		err    error
	}
	done := make(chan answered, 1)
	go func() {
		result, err := b.Ask(ctx, AskRequest{
			Question: "ship it?",
			Options:  []string{"yes", "no"},
			Timeout:  300 * time.Millisecond,
		})
		done <- answered{result, err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Ask() error = %v, want the ordinary timed-out answer", got.err)
		}
		if !got.result.TimedOut {
			t.Errorf("Ask() = %+v, want it to report the timeout it promised", got.result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Ask() never came back: its own deadline did not bound the backlog it was collecting")
	}
}

// A conversation the walk could not reach holds replies that are in neither
// queue. A question asked over the top of them is a question the owner has
// already answered somewhere else, so the walk that brings them counts as a
// backlog — once per question, because it is a round trip.
func TestAQuestionCollectsTheRepliesAWalkCouldNotReach(t *testing.T) {
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
	// More than two passes can reach, so one is still waiting by the time the
	// question is asked: the first catch-up walks its budget, the last look
	// behind it walks another, and one conversation is left.
	cursors := map[threadKey]string{}
	for i := 0; i < 2*maxThreadsPerCatchUp+1; i++ {
		ts := "50.0000" + fmt.Sprintf("%02d", i)
		if err := store.SetThread("CPROJ", ts, "60.000000"); err != nil {
			t.Fatal(err)
		}
		cursors[threadKey{"CPROJ", ts}] = "60.000000"
	}
	order := threadWalkOrder(cursors, nil)
	target := order[len(order)-1] // the one a full walk skips

	api := &fakeAPI{
		botUserID:      testBotUser,
		questionTS:     askTS,
		postTS:         "100.000900",
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream(), quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	// The first catch-up reads what it has budget for and leaves one behind.
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// The owner says something in one of those conversations, and the walk
	// that would read it has run out of budget: the reply is in Slack, in
	// neither queue, and only a walk will bring it.
	api.mu.Lock()
	api.replies = []candidate{
		{Channel: "CPROJ", User: testOwner, Text: "said in the conversation the walk skipped", TS: "70.000000", ThreadTS: target.threadTS},
	}
	api.mu.Unlock()

	b.mu.Lock()
	b.threadsSkipped = true
	b.skippedThreads = map[threadKey]struct{}{target: {}}
	b.mu.Unlock()

	result, err := b.Ask(ctx, AskRequest{
		Question: "ship it?",
		Options:  []string{"yes", "no"},
		Timeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if len(result.Messages) != 1 || result.Messages[0].TS != "70.000000" {
		t.Errorf("Ask() = %v, want the reply from the conversation the walk could not reach", texts(result.Messages))
	}
	// Handed over instead of the question, not alongside the timeout it would
	// otherwise have run out to: the owner has already said something, and the
	// question was about to talk over it.
	if result.TimedOut {
		t.Errorf("Ask() timed out with the reply attached, want the question interrupted by it")
	}

	// A wakeup that is not about conversations buys no walk. The walk is a
	// round trip, and the question looks for skipped conversations once; after
	// that only a walk that leaves some behind says so again, and each of those
	// reads the set and shrinks it.
	api.mu.Lock()
	api.replies = nil
	api.mu.Unlock()
	b.mu.Lock()
	b.threadsSkipped = true
	b.skippedThreads = map[threadKey]struct{}{target: {}}
	b.mu.Unlock()

	reads := len(api.replyCallsSnapshot())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := b.Ask(ctx, AskRequest{
			Question: "and now?",
			Options:  []string{"yes", "no"},
			Timeout:  time.Second,
		}); err != nil {
			t.Errorf("Ask() error = %v", err)
		}
	}()
	waitForQuestion(b)

	// Woken three times by something that is not a conversation: a loss
	// marker, which wakes the subscribers and hands over no messages.
	for i := 0; i < 3; i++ {
		b.noteReactionsDropped()
		time.Sleep(20 * time.Millisecond)
	}
	<-done

	if got := len(api.replyCallsSnapshot()) - reads; got > maxThreadsPerCatchUp {
		t.Errorf("the second question cost %d thread reads, want one walk at most", got)
	}
}

// More conversations left unread than one walk can read. The first look brings
// what it can reach, and the walk that leaves some behind says so again — so a
// reply in the last of them interrupts the question rather than arriving with
// the timeout it ran out to.
func TestARepliesBeyondOneWalkStillInterruptTheQuestion(t *testing.T) {
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
	// Enough that one walk cannot reach them all.
	cursors := map[threadKey]string{}
	for i := 0; i < 2*maxThreadsPerCatchUp+1; i++ {
		ts := "50.0000" + fmt.Sprintf("%02d", i)
		if err := store.SetThread("CPROJ", ts, "60.000000"); err != nil {
			t.Fatal(err)
		}
		cursors[threadKey{"CPROJ", ts}] = "60.000000"
	}
	order := threadWalkOrder(cursors, nil)
	last := order[len(order)-1]

	api := &fakeAPI{
		botUserID:      testBotUser,
		questionTS:     askTS,
		postTS:         "100.000900",
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "old")}},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream(), quiet: true})
	t.Cleanup(func() { _ = b.Close() })

	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// The owner says something in the conversation the walks reach last, and
	// every conversation is waiting again.
	api.mu.Lock()
	api.replies = []candidate{
		{Channel: "CPROJ", User: testOwner, Text: "beyond the first walk", TS: "70.000000", ThreadTS: last.threadTS},
	}
	api.mu.Unlock()

	waiting := make(map[threadKey]struct{}, len(cursors))
	for key := range cursors {
		waiting[key] = struct{}{}
	}
	b.mu.Lock()
	b.threadsSkipped = true
	b.skippedThreads = waiting
	b.mu.Unlock()

	started := time.Now()
	result, err := b.Ask(ctx, AskRequest{
		Question: "ship it?",
		Options:  []string{"yes", "no"},
		Timeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.TimedOut {
		t.Errorf("Ask() timed out after %v, want the reply to interrupt it: one walk cannot reach every conversation", time.Since(started))
	}
	if len(result.Messages) != 1 || result.Messages[0].TS != "70.000000" {
		t.Errorf("Ask() = %v, want the reply from the conversation the walks reach last", texts(result.Messages))
	}
}

// The timeout a question is given covers the whole call, posting included. A
// post is a Slack request like any other, and a clock started after it is one
// the call can outlast without ever having waited for an answer.
func TestAQuestionsTimeoutCoversThePostAsWell(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// Slack takes its time over the post, and the question's own timeout is
	// shorter than the bound posting would otherwise get.
	api.mu.Lock()
	api.questionDelay = time.Second
	api.mu.Unlock()

	started := time.Now()
	_, err := b.Ask(ctx, AskRequest{
		Question: "ship it?",
		Options:  []string{"yes", "no"},
		Timeout:  300 * time.Millisecond,
	})
	took := time.Since(started)

	if err == nil {
		t.Fatal("Ask() returned no error, want the post given up on")
	}
	if took > 2*time.Second {
		t.Errorf("Ask() took %v for a question given 300ms; posting is not bounded by the call's own timeout", took)
	}
}

// A question whose post was given up on can still be standing in the channel,
// with buttons nothing can answer: the timestamp that would retire it went
// with the request. The next question looks for it.
func TestAQuestionWhosePostWasAbandonedIsRetiredLater(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	// Known before the connection opens: the bridge reads its own user ID
	// when it connects, and recognising its own message is what the search
	// below turns on.
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	api.mu.Lock()
	api.questionDelay = time.Second
	api.mu.Unlock()

	if _, err := b.Ask(ctx, AskRequest{
		Question: "ship it?",
		Options:  []string{"yes", "no"},
		Timeout:  200 * time.Millisecond,
	}); err == nil {
		t.Fatal("Ask() returned no error, want the post given up on")
	}

	// It landed after all, which is what an abandoned request does — and what
	// Slack stored is not what was sent: the ampersand and the link have been
	// rewritten, which is why the search does not go by the text.
	landed := slackTS(time.Now())
	api.mu.Lock()
	api.questionDelay = 0
	api.channelHistory = map[string][]candidate{testChannel: {
		// Older than the attempt, and answered: not this call's business.
		{Channel: testChannel, User: testBotUser, Text: "ship it?\n\n✅ yes", TS: "100.000300"},
		{Channel: testChannel, User: testBotUser, Text: "ship it? see &amp; &lt;https://example.com&gt;", TS: landed, HasAskButtons: true},
	}}
	api.mu.Unlock()

	// The next question looks for it before putting another one up.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := b.Ask(ctx, AskRequest{
			Question: "and now?",
			Options:  []string{"yes", "no"},
			Timeout:  300 * time.Millisecond,
		}); err != nil {
			t.Errorf("Ask() error = %v", err)
		}
	}()
	<-done

	var retired, touchedOld bool
	api.mu.Lock()
	for _, u := range api.resolutions {
		switch u.TS {
		case landed:
			retired = true
		case "100.000300":
			touchedOld = true
		}
	}
	api.mu.Unlock()
	if !retired {
		t.Error("the question left standing by an abandoned post still has its buttons; a click on it answers nothing")
	}
	if touchedOld {
		t.Error("a question from before the attempt was rewritten as expired")
	}
}

// A click that settles a question at its deadline is an answer in hand. The
// backlog collected on the way out has a budget of nothing left, and the
// courtesy the timed-out path gets would spend it past the timeout the caller
// asked for.
func TestAQuestionAnsweredAtTheDeadlineStillReturnsOnTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// Slack stops answering history, and a catch-up is due — so the backlog
	// collection has somewhere to hang.
	gate := make(chan struct{})
	defer close(gate)
	api.mu.Lock()
	api.historyGate = gate
	api.mu.Unlock()
	b.mu.Lock()
	b.needCatchUp = true
	b.mu.Unlock()

	type answered struct {
		result AskResult
		err    error
	}
	done := make(chan answered, 1)
	go func() {
		result, err := b.Ask(ctx, AskRequest{
			Question:          "ship it?",
			Options:           []string{"yes", "no"},
			Timeout:           400 * time.Millisecond,
			InterruptDisabled: true,
		})
		done <- answered{result, err}
	}()
	waitForQuestion(b)

	// Clicked as the question runs out.
	started := time.Now()
	time.Sleep(350 * time.Millisecond)
	stream.interactions <- click(testOwner, askTS, 0)

	select {
	case got := <-done:
		took := time.Since(started)
		if got.err != nil {
			t.Fatalf("Ask() error = %v", got.err)
		}
		if got.result.ChoiceIndex != 0 && !got.result.TimedOut {
			t.Errorf("Ask() = %+v, want the answer or the timeout, not something else", got.result)
		}
		// Its own timeout and a moment, not its own timeout and a courtesy
		// spent on a Slack that is not answering.
		if took > 400*time.Millisecond+askLastLookWait/2 {
			t.Errorf("Ask() took %v for a question given 400ms; a budget already spent bought another look", took)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Ask() never came back: a question with nothing left of its budget still spent a courtesy on Slack")
	}
}

// The search for a question an abandoned post may have left is inside the
// question's own timeout, like everything else the call does: it may never have
// landed, and looking for it is not worth the promise the caller was given.
func TestTheSearchForAnAbandonedQuestionIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// A question was given up on mid-post, and Slack has stopped answering
	// history — which is where the search for it goes.
	b.noteOrphanQuestion(testChannel, "", time.Now())
	gate := make(chan struct{})
	defer close(gate)
	api.mu.Lock()
	api.historyGate = gate
	api.mu.Unlock()

	started := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := b.Ask(ctx, AskRequest{
			Question: "and now?",
			Options:  []string{"yes", "no"},
			Timeout:  300 * time.Millisecond,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Ask() error = %v", err)
		}
		if took := time.Since(started); took > 300*time.Millisecond+askLastLookWait {
			t.Errorf("Ask() took %v for a question given 300ms; the search for the abandoned one is not inside the timeout", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Ask() never came back: the search for a question that may never have landed is not bounded at all")
	}
}

// A question that has already been answered is not an abandoned one. What
// Slack shows for a live question is its text and nothing else; a retired one
// carries what retired it after that, so the match is exact.
func TestAnAnsweredQuestionIsNotMistakenForAnAbandonedOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// Inside the window the search looks at, and already answered: the mark
	// under it is what says so.
	attempted := time.Now()
	answeredTS := slackTS(attempted.Add(time.Millisecond))
	api.mu.Lock()
	api.channelHistory = map[string][]candidate{testChannel: {
		{Channel: testChannel, User: testBotUser, Text: "ship it?\n\n✅ yes", TS: answeredTS},
	}}
	api.mu.Unlock()
	b.noteOrphanQuestion(testChannel, "", attempted)

	if _, err := b.Ask(ctx, AskRequest{
		Question: "and now?",
		Options:  []string{"yes", "no"},
		Timeout:  200 * time.Millisecond,
	}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	api.mu.Lock()
	defer api.mu.Unlock()
	for _, r := range api.resolutions {
		if r.TS == answeredTS {
			t.Error("a question the owner had already answered was rewritten as expired")
		}
	}
}

// A question asked inside a conversation is looked for there. A reply is in no
// channel history, so searching the channel would never find it.
func TestAnAbandonedQuestionInAThreadIsLookedForInTheThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	attempted := time.Now()
	orphanTS := slackTS(attempted.Add(time.Millisecond))
	api.mu.Lock()
	// A long conversation, with the abandoned question at the end of it.
	api.replies = nil
	for i := 0; i < 50; i++ {
		api.replies = append(api.replies, candidate{
			Channel: testChannel, User: colleague, Text: "chatter",
			TS: fmt.Sprintf("100.0006%02d", i), ThreadTS: "100.000500",
		})
	}
	api.replies = append(api.replies, candidate{
		Channel: testChannel, User: testBotUser, Text: "ship it?", TS: orphanTS, ThreadTS: "100.000500",
		HasAskButtons: true,
	})
	api.mu.Unlock()
	b.noteOrphanQuestion(testChannel, "100.000500", attempted)

	if _, err := b.Ask(ctx, AskRequest{
		Question: "and now?",
		Options:  []string{"yes", "no"},
		Timeout:  200 * time.Millisecond,
	}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	var retired bool
	api.mu.Lock()
	for _, r := range api.resolutions {
		if r.TS == orphanTS {
			retired = true
		}
	}
	api.mu.Unlock()
	if !retired {
		t.Error("the question left in a conversation still has its buttons; the search looked in the channel, where a reply never is")
	}
}

// Nothing is posted by a call that has already run out of time. The search for
// an abandoned question can spend the whole of a short timeout, and putting
// buttons in the channel for a call that is over is worse than not asking.
func TestNoQuestionIsPostedWithNoTimeLeft(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// The search for the abandoned question takes longer than the question
	// about to be asked has to live.
	b.noteOrphanQuestion(testChannel, "", time.Now())
	gate := make(chan struct{})
	api.mu.Lock()
	api.historyGate = gate
	posted := len(api.questions)
	api.mu.Unlock()
	go func() {
		time.Sleep(120 * time.Millisecond)
		close(gate)
	}()

	result, err := b.Ask(ctx, AskRequest{
		Question: "and now?",
		Options:  []string{"yes", "no"},
		Timeout:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if !result.TimedOut {
		t.Errorf("Ask() = %+v, want the timeout it had already spent", result)
	}

	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.questions) != posted {
		t.Error("a question went up for a call that was already over; its buttons answer nobody")
	}
}

// The search for an abandoned question and the retirement that follows it are
// one budget between them, not one each. A Slack that answers the first slowly
// and the second not at all would otherwise hold the question that paid for
// both.
func TestRetiringAnAbandonedQuestionIsInsideTheSameBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	attempted := time.Now()
	orphanTS := slackTS(attempted.Add(time.Millisecond))
	history := make(chan struct{})
	resolve := make(chan struct{})
	defer close(resolve)
	api.mu.Lock()
	api.channelHistory = map[string][]candidate{testChannel: {
		{Channel: testChannel, User: testBotUser, Text: "ship it?", TS: orphanTS, HasAskButtons: true},
	}}
	api.historyGate = history
	api.resolveGate = resolve
	api.mu.Unlock()
	b.noteOrphanQuestion(testChannel, "", attempted)

	// The search answers late, and the retirement never does.
	go func() {
		time.Sleep(300 * time.Millisecond)
		close(history)
	}()

	started := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := b.Ask(ctx, AskRequest{
			Question: "and now?",
			Options:  []string{"yes", "no"},
			Timeout:  time.Second,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Ask() error = %v", err)
		}
		if took := time.Since(started); took > time.Second+askLastLookWait {
			t.Errorf("Ask() took %v for a question given a second; the search and the retirement each took a budget of their own", took)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Ask() never came back: retiring the abandoned question is not inside the budget the search shares")
	}
}

// What identifies an abandoned question is the block the bridge puts its
// buttons in, not "the newest thing I posted". After a post that failed the
// bridge posts other things — the indicator saying it is working, the reply to
// whatever prompted the question — and expiring one of those rewrites a message
// the owner is reading.
func TestOnlyAQuestionIsRetiredAsAnAbandonedOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	attempted := time.Now()
	orphanTS := slackTS(attempted.Add(time.Millisecond))
	laterTS := slackTS(attempted.Add(2 * time.Millisecond))
	api.mu.Lock()
	api.channelHistory = map[string][]candidate{testChannel: {
		{Channel: testChannel, User: testBotUser, Text: "ship it?", TS: orphanTS, HasAskButtons: true},
		// Posted after it, by the bridge, and not a question: the indicator,
		// or an answer the agent gave.
		{Channel: testChannel, User: testBotUser, Text: "⏳ Working… (0s)", TS: laterTS},
	}}
	api.mu.Unlock()
	b.noteOrphanQuestion(testChannel, "", attempted)

	if _, err := b.Ask(ctx, AskRequest{
		Question: "and now?",
		Options:  []string{"yes", "no"},
		Timeout:  300 * time.Millisecond,
	}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	api.mu.Lock()
	defer api.mu.Unlock()
	var retiredOrphan bool
	for _, r := range api.resolutions {
		switch r.TS {
		case orphanTS:
			retiredOrphan = true
		case laterTS:
			t.Error("a message the bridge posted after the question was rewritten as an expired question")
		}
	}
	if !retiredOrphan {
		t.Error("the abandoned question was not retired")
	}
}

// The moment of the attempt is this machine's clock and the timestamps are
// Slack's, and both ends of a window are exclusive. A question posted in the
// same instant must not fall outside a window cut to it.
func TestAnAbandonedQuestionIsFoundDespiteTheClocksDisagreeing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// Slack's clock is a moment behind this one: the question it stored is
	// stamped before the attempt was made.
	attempted := time.Now()
	orphanTS := slackTS(attempted.Add(-200 * time.Millisecond))
	api.mu.Lock()
	api.channelHistory = map[string][]candidate{testChannel: {
		{Channel: testChannel, User: testBotUser, Text: "ship it?", TS: orphanTS, HasAskButtons: true},
	}}
	api.mu.Unlock()
	b.noteOrphanQuestion(testChannel, "", attempted)

	if _, err := b.Ask(ctx, AskRequest{
		Question: "and now?",
		Options:  []string{"yes", "no"},
		Timeout:  300 * time.Millisecond,
	}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	var retired bool
	api.mu.Lock()
	for _, r := range api.resolutions {
		if r.TS == orphanTS {
			retired = true
		}
	}
	api.mu.Unlock()
	if !retired {
		t.Error("a question stamped a moment before the attempt was outside the window; the clocks are not the same one")
	}
}

// A channel that has been busy since the attempt fills the newest end of the
// page, which is the end history counts its limit from. The window has an
// upper end for that: as long after the attempt as the post could have taken,
// and no longer.
func TestAnAbandonedQuestionIsFoundBehindABusyChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := askBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = testBotUser
	api.mu.Unlock()
	if _, err := b.Wait(ctx, 50*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	attempted := time.Now()
	orphanTS := slackTS(attempted.Add(time.Millisecond))
	history := []candidate{
		{Channel: testChannel, User: testBotUser, Text: "ship it?", TS: orphanTS, HasAskButtons: true},
	}
	// Twenty-five messages from everybody else, after it and well past the
	// window's far end.
	for i := 0; i < 25; i++ {
		history = append(history, candidate{
			Channel: testChannel, User: colleague, Text: "chatter",
			TS: slackTS(attempted.Add(askPostTimeout + orphanClockSlack + time.Duration(i+1)*time.Second)),
		})
	}
	api.mu.Lock()
	api.channelHistory = map[string][]candidate{testChannel: history}
	api.mu.Unlock()
	b.noteOrphanQuestion(testChannel, "", attempted)

	if _, err := b.Ask(ctx, AskRequest{
		Question: "and now?",
		Options:  []string{"yes", "no"},
		Timeout:  300 * time.Millisecond,
	}); err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	var retired bool
	api.mu.Lock()
	for _, r := range api.resolutions {
		if r.TS == orphanTS {
			retired = true
		}
	}
	api.mu.Unlock()
	if !retired {
		t.Error("the abandoned question was behind a busy channel's later traffic; the window has to have a far end as well as a near one")
	}
}
