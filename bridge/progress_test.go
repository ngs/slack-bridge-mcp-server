package bridge

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// progressLabel is long enough to be unmistakable in the message text and
// short enough to read in a failure message.
const progressLabel = "release chain: waiting for CI"

// lastIndicatorUpdate returns the most recent chat.update the indicator made,
// which is what the owner is looking at right now.
func lastIndicatorUpdate(api *fakeAPI) string {
	updates := api.snapshotUpdates()
	if len(updates) == 0 {
		return ""
	}
	return updates[len(updates)-1].Text
}

// The label is not a one-off edit: once given, it belongs to the indicator, so
// it has to survive every tick until the turn ends.
func TestProgressLabelRidesTheIndicatorUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	result, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: ""})
	if err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	if !result.OK {
		t.Errorf("Progress() = %+v, want ok on a running indicator", result)
	}
	if result.TS != "100.000900" {
		t.Errorf("Progress() ts = %q, want the indicator's own message", result.TS)
	}

	// Two updates carrying the label, so this is the standing text and not a
	// single edit the next tick would wipe out.
	eventually(t, "the label to keep appearing in the ticker updates", func() bool {
		labelled := 0
		for _, u := range api.snapshotUpdates() {
			if strings.Contains(u.Text, progressLabel) {
				labelled++
			}
		}
		return labelled >= 2
	})

	if got := lastIndicatorUpdate(api); !strings.Contains(got, "Working") {
		t.Errorf("indicator text = %q, want the elapsed time kept alongside the label", got)
	}
}

// The agent has just said the work will take a while, so the silence the grace
// period protects has nothing left to protect: the indicator posts now, label
// and all, rather than sitting out a wait for an answer that is not coming.
func TestProgressCutsTheGracePeriodShort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	// Far longer than the test is willing to wait, so a post that does arrive
	// can only be the one this call asked for.
	b.cfg.IndicatorGrace = 60 * time.Second

	waitForMessages(ctx, t, b)
	if got := indicatorPosts(api); len(got) != 0 {
		t.Fatalf("indicator posts = %+v before the grace period is up, want none", got)
	}

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: ""}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	eventually(t, "the indicator to post without waiting out the grace period", func() bool {
		return len(indicatorPosts(api)) == 1
	})
	if got := indicatorPosts(api)[0].Text; !strings.Contains(got, progressLabel) {
		t.Errorf("indicator text = %q, want the label the agent just set", got)
	}
}

// Cutting the grace short must not cut the handover short too. The previous
// indicator's chat.delete is what keeps two "⏳ Working…" messages from being
// visible at once, and it outranks any hurry this call is in.
func TestProgressStillWaitsForTheOutgoingIndicator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := indicatorBridge(ctx, t)

	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()

	api.mu.Lock()
	api.deleteGate = gate
	api.mu.Unlock()

	waitForMessages(ctx, t, b)
	eventually(t, "the first indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	// A second turn begins while the first indicator is still being deleted.
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300", User: testOwner, Text: "and another thing"}}
	waitForMessages(ctx, t, b)
	eventually(t, "the outgoing indicator to start deleting", func() bool { return len(api.snapshotDeletes()) == 1 })

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: ""}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	time.Sleep(6 * testGrace)
	if got := indicatorPosts(api); len(got) != 1 {
		t.Fatalf("indicator posts = %d while the previous message was still being deleted, want 1", len(got))
	}

	release()
	eventually(t, "the replacement to post once the channel is clear", func() bool {
		return len(indicatorPosts(api)) == 2
	})
	if got := indicatorPosts(api)[1].Text; !strings.Contains(got, progressLabel) {
		t.Errorf("replacement indicator text = %q, want the label", got)
	}
}

// The label wakes the indicator's goroutine, and the reply that ends the turn
// can land before it gets there — leaving "post now" and "stop" ready at the
// same moment. select picks between ready cases at random, so this only failed
// half the time, and what it left behind was a "⏳ Working…" posted after the
// answer it was standing in for.
func TestALabelledIndicatorStoppedBeforeItPostsStaysQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	// Long enough that only the label could bring the post forward.
	b.cfg.IndicatorGrace = 60 * time.Second

	waitForMessages(ctx, t, b)

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	// The answer arrives before the goroutine has acted on the label.
	if _, err := b.Post(ctx, PostRequest{Text: "done already"}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	time.Sleep(10 * testGrace)
	if got := indicatorPosts(api); len(got) != 0 {
		t.Errorf("indicator posted %+v after the turn had ended, want nothing", got)
	}
}

// Long work does not always start inside a turn: a wait that timed out leaves
// the agent working with nothing counting in the channel. Saying so has to be
// enough to start the indicator.
func TestProgressStartsAnIndicatorWhenNoneIsRunning(t *testing.T) {
	t.Run("on the channel surface", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		b, api, _ := indicatorBridge(ctx, t)
		if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: ""}); err != nil {
			t.Fatalf("Progress() error = %v", err)
		}

		eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })
		posted := indicatorPosts(api)[0]
		if posted.ThreadTS != "" {
			t.Errorf("indicator posted as %+v, want it on the channel surface", posted)
		}
		if !strings.Contains(posted.Text, progressLabel) {
			t.Errorf("indicator text = %q, want the label", posted.Text)
		}

		// It is an ordinary indicator from here: the reply ends it.
		if _, err := b.Post(ctx, PostRequest{Text: "done", ThreadTS: ""}); err != nil {
			t.Fatalf("Post() error = %v", err)
		}
		eventually(t, "the indicator to be deleted", func() bool { return len(api.snapshotDeletes()) == 1 })
	})

	t.Run("inside the thread it was given", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		b, api, _ := indicatorBridge(ctx, t)
		if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: "100.000200"}); err != nil {
			t.Fatalf("Progress() error = %v", err)
		}

		eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })
		if got := indicatorPosts(api)[0]; got.ThreadTS != "100.000200" {
			t.Errorf("indicator posted as %+v, want it in thread 100.000200", got)
		}

		// Going back to waiting ends this turn too, whoever started it.
		if _, err := b.Wait(ctx, MaxWaitTimeout); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		eventually(t, "the indicator to be deleted", func() bool { return len(api.snapshotDeletes()) == 1 })
	})
}

// A thread_ts alongside a running indicator used to be treated as stale
// information and ignored, which is what put a status line under the wrong
// conversation. It now names where the status belongs; the tests for that, and
// for the calls that still move nothing, are at the end of this file.

// The status line answers "what is happening now", so the newest answer is the
// only one worth showing.
func TestProgressReplacesTheLabel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if _, err := b.Progress(ctx, ProgressRequest{Text: "waiting for CI", ThreadTS: ""}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	eventually(t, "the first label to reach the channel", func() bool {
		return strings.Contains(lastIndicatorUpdate(api), "waiting for CI")
	})

	if _, err := b.Progress(ctx, ProgressRequest{Text: "publishing the release", ThreadTS: ""}); err != nil {
		t.Fatalf("second Progress() error = %v", err)
	}
	eventually(t, "the second label to replace the first", func() bool {
		got := lastIndicatorUpdate(api)
		return strings.Contains(got, "publishing the release") && !strings.Contains(got, "waiting for CI")
	})
}

// The label describes one piece of work. When that turn ends the label ends
// with it, or the next turn opens claiming to be waiting on something finished
// hours ago.
func TestTheLabelRetiresWithTheIndicator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: ""}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	eventually(t, "the label to reach the channel", func() bool {
		return strings.Contains(lastIndicatorUpdate(api), progressLabel)
	})

	// A new turn, and with it a new indicator.
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300", User: testOwner, Text: "and another thing"}}
	waitForMessages(ctx, t, b)

	eventually(t, "the next turn's indicator to be posted", func() bool { return len(indicatorPosts(api)) == 2 })
	if got := indicatorPosts(api)[1].Text; strings.Contains(got, progressLabel) {
		t.Errorf("next indicator text = %q, want it label-less: that work is over", got)
	}
}

// The operator turned the indicator off, so there is nowhere to show a label.
// Nothing is broken, and the call says so rather than failing work the agent
// started in good faith.
func TestProgressDoesNothingWhenTheIndicatorIsOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	b.cfg.IndicatorDisabled = true

	result, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: ""})
	if err != nil {
		t.Fatalf("Progress() error = %v, want the call to succeed quietly", err)
	}
	if result.OK {
		t.Errorf("Progress() = %+v, want ok false: the label has nowhere to appear", result)
	}

	time.Sleep(4 * testGrace)
	if got := indicatorPosts(api); len(got) != 0 {
		t.Errorf("indicator posted %+v with the feature off, want nothing", got)
	}
}

// With nowhere to put a label there is nothing to do, and finding that out must
// not drag Slack into it: a session with the indicator off and no credentials
// at all still gets a quiet answer rather than a configuration error.
func TestProgressWithTheIndicatorOffDoesNotConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connector := &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()}
	b := New(ctx, Config{StateDir: t.TempDir(), IndicatorDisabled: true}, connector)
	t.Cleanup(func() { _ = b.Close() })

	result, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: ""})
	if err != nil {
		t.Fatalf("Progress() error = %v, want the call to succeed quietly", err)
	}
	if result.OK {
		t.Errorf("Progress() = %+v, want ok false", result)
	}
	if got := connector.connectCount(); got != 0 {
		t.Errorf("the bridge connected %d times, want it to stay offline", got)
	}
}

// A label made of nothing is not a status line, and posting "⏳ Working… (3s) —"
// would be worse than saying nothing at all.
func TestProgressRequiresSomethingToSay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)

	for _, text := range []string{"", "   ", "\n\t"} {
		if _, err := b.Progress(ctx, ProgressRequest{Text: text, ThreadTS: ""}); err == nil {
			t.Errorf("Progress(%q) = nil error, want it refused", text)
		}
	}

	time.Sleep(4 * testGrace)
	if got := indicatorPosts(api); len(got) != 0 {
		t.Errorf("indicator posted %+v for an empty label, want nothing", got)
	}
}

func TestSanitizeProgressLabel(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain text is left alone", "waiting for CI", "waiting for CI"},
		{"newlines become spaces", "waiting for CI\non the release branch", "waiting for CI on the release branch"},
		{"runs of whitespace collapse", "waiting\t\t for   CI", "waiting for CI"},
		{"surrounding whitespace goes", "  waiting for CI \n", "waiting for CI"},
		{"control characters go", "waiting\x00for\x07CI", "waiting for CI"},
		{"zero-width and bidi marks go too", "waiting\u200bfor\u202eCI", "waiting for CI"},
		{"whitespace only comes back empty", " \n\t ", ""},
		{"multibyte text survives", "リリース待ち", "リリース待ち"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeProgressLabel(tt.in); got != tt.want {
				t.Errorf("sanitizeProgressLabel(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// A model that answers with a paragraph, or pastes a stack trace, costs the
// owner one line rather than a wall of text — and the cut is counted in runes,
// so a label written in Japanese gets the same room as an English one.
func TestProgressLabelIsTruncated(t *testing.T) {
	long := strings.Repeat("a", maxProgressLabel+50)
	got := sanitizeProgressLabel(long)

	if n := utf8.RuneCountInString(got); n != maxProgressLabel {
		t.Errorf("truncated label is %d runes, want %d", n, maxProgressLabel)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncated label = %q, want it to end in an ellipsis", got)
	}

	multibyte := strings.Repeat("待", maxProgressLabel+50)
	truncated := sanitizeProgressLabel(multibyte)
	if n := utf8.RuneCountInString(truncated); n != maxProgressLabel {
		t.Errorf("truncated multibyte label is %d runes, want %d", n, maxProgressLabel)
	}
	if !utf8.ValidString(truncated) {
		t.Errorf("truncated multibyte label = %q, want it cut on a character boundary", truncated)
	}

	// Exactly at the cap is not too long, so nothing is dropped.
	exact := strings.Repeat("b", maxProgressLabel)
	if got := sanitizeProgressLabel(exact); got != exact {
		t.Errorf("a label of exactly %d runes was altered: %q", maxProgressLabel, got)
	}

	// A model that pastes a log file gets the same one line as everyone else,
	// and the sanitizer stops reading it long before the end.
	huge := strings.Repeat("stack frame ", 100_000)
	if n := utf8.RuneCountInString(sanitizeProgressLabel(huge)); n != maxProgressLabel {
		t.Errorf("a %d-rune label came back as %d runes, want %d", utf8.RuneCountInString(huge), n, maxProgressLabel)
	}
}

// indicatorLocation reports where the running indicator is posting, so a test
// can see a move that has yet to reach Slack.
func indicatorLocation(b *Bridge) (channel, threadTS string, running bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.indicator == nil {
		return "", "", false
	}
	return b.indicator.channel, b.indicator.threadTS, true
}

// indicatorStartedAt is the clock the elapsed counter runs off.
func indicatorStartedAt(t *testing.T, b *Bridge) time.Time {
	t.Helper()

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.indicator == nil {
		t.Fatal("no indicator is running")
	}
	return b.indicator.startedAt
}

// The bug this fixes: the indicator starts wherever the owner last spoke, and
// with two topics interleaved that is not where the work is. A label naming
// the conversation it belongs to has to take the indicator with it.
func TestProgressMovesTheIndicatorToTheConversationItNames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to post where the owner last spoke", func() bool {
		return len(indicatorPosts(api)) == 1
	})
	if got := indicatorPosts(api)[0]; got.ThreadTS != "" {
		t.Fatalf("the first indicator went to %+v, want the channel surface", got)
	}

	// The agent is actually working on the other topic, which lives in a
	// thread.
	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, Channel: testChannel, ThreadTS: "100.000700"}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	eventually(t, "the indicator to reappear in the thread it was moved to", func() bool {
		return len(indicatorPosts(api)) == 2
	})
	moved := indicatorPosts(api)[1]
	if moved.ThreadTS != "100.000700" || moved.Channel != testChannel {
		t.Errorf("moved indicator posted as %+v, want it in thread 100.000700 of %s", moved, testChannel)
	}
	if !strings.Contains(moved.Text, progressLabel) {
		t.Errorf("moved indicator text = %q, want it to carry the label", moved.Text)
	}

	// The message left behind has to go, or the move is just a second
	// indicator.
	eventually(t, "the indicator left behind to be deleted", func() bool {
		return len(api.snapshotDeletes()) == 1
	})
	if got := api.snapshotDeletes()[0].TS; got != "100.000900" {
		t.Errorf("deleted ts = %q, want the indicator that was standing in the wrong conversation", got)
	}
}

// A move before the first post is the common one: the grace period is ten
// seconds and the agent works out what it is doing well inside that. Nothing
// has been posted, so nothing may be deleted, and only one message may ever
// appear.
func TestProgressMovesAnIndicatorThatHasNotPostedYet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	// Long enough that the only thing that can bring a post forward is the
	// label this test sets.
	b.cfg.IndicatorGrace = 60 * time.Second

	waitForMessages(ctx, t, b)
	if got := indicatorPosts(api); len(got) != 0 {
		t.Fatalf("indicator posts = %+v inside the grace period, want none", got)
	}

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: "100.000700"}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	eventually(t, "the indicator to post in the thread it was moved to", func() bool {
		return len(indicatorPosts(api)) == 1
	})
	if got := indicatorPosts(api)[0]; got.ThreadTS != "100.000700" {
		t.Errorf("indicator posted as %+v, want it in the thread the label named", got)
	}

	// Give the abandoned indicator every chance to post or delete something.
	time.Sleep(10 * testGrace)
	if got := indicatorPosts(api); len(got) != 1 {
		t.Errorf("indicator posts = %+v, want exactly one: the one that never posted has nothing to show", got)
	}
	if got := api.snapshotDeletes(); len(got) != 0 {
		t.Errorf("deletes = %+v, want none: there was no message to delete", got)
	}
}

// The counter measures how long the owner has been waiting, not how long this
// message has been on screen. Moving the status must not tell them the work
// just started.
func TestMovingTheIndicatorKeepsTheElapsedTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the first indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	// A turn that has been going for a while, set up the way a restored
	// indicator is: the clock is fixed before the goroutine starts.
	started := time.Now().Add(-5 * time.Minute)
	b.startIndicatorAt(started, testChannel, "")
	eventually(t, "the long-running indicator to post", func() bool { return len(indicatorPosts(api)) == 2 })

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: "100.000700"}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	eventually(t, "the moved indicator to post", func() bool { return len(indicatorPosts(api)) == 3 })
	moved := indicatorPosts(api)[2]
	if !strings.Contains(moved.Text, "5m") {
		t.Errorf("moved indicator text = %q, want the elapsed time carried over rather than restarted", moved.Text)
	}
	if got := indicatorStartedAt(t, b); !got.Equal(started) {
		t.Errorf("moved indicator startedAt = %v, want the original %v", got, started)
	}
}

// Every call that does not name a conversation has to behave exactly as it did
// before any of this existed: label the indicator where it is, in place.
func TestProgressWithoutATargetLeavesTheIndicatorWhereItIs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridgeWith(ctx, t, []candidate{
		ownerMsg("100.000100", "already answered"),
		{Channel: testChannel, User: testOwner, Text: "asked in a thread", TS: "100.000200", ThreadTS: "100.000150"},
	})
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	result, err := b.Progress(ctx, ProgressRequest{Text: progressLabel})
	if err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	if result.TS != "100.000900" {
		t.Errorf("Progress() ts = %q, want the indicator that was already running", result.TS)
	}

	eventually(t, "the label to reach the message in place", func() bool {
		return strings.Contains(lastIndicatorUpdate(api), progressLabel)
	})

	_, thread, running := indicatorLocation(b)
	if !running || thread != "100.000150" {
		t.Errorf("indicator thread = %q (running %v), want it left in the thread the owner spoke in", thread, running)
	}
	if got := api.snapshotDeletes(); len(got) != 0 {
		t.Errorf("deletes = %+v, want none: a label with no target moves nothing", got)
	}
	if got := indicatorPosts(api); len(got) != 1 {
		t.Errorf("indicator posts = %+v, want the one message, updated in place", got)
	}
}

// Naming the conversation the indicator is already in is not a move, and must
// not cost the owner a message that disappears and comes back.
func TestProgressNamingTheCurrentConversationDoesNotMoveAnything(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	result, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, Channel: testChannel})
	if err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	if result.TS != "100.000900" {
		t.Errorf("Progress() ts = %q, want the running indicator's own message", result.TS)
	}

	time.Sleep(10 * testGrace)
	if got := indicatorPosts(api); len(got) != 1 {
		t.Errorf("indicator posts = %+v, want the original left alone", got)
	}
	if got := api.snapshotDeletes(); len(got) != 0 {
		t.Errorf("deletes = %+v, want none", got)
	}
}

// A thread named without a channel moves within the conversation the indicator
// is already in. The alternative — reading the missing channel as the home
// channel, the way every other tool does — would drag the status out of the
// channel the agent is working in on a call that never mentioned it.
func TestProgressMovesWithinTheCurrentChannelWhenOnlyAThreadIsNamed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	// The turn is happening somewhere that is not the home channel.
	b.startIndicatorAt(time.Now(), "C0ELSEWHERE", "100.000500")
	eventually(t, "the indicator to post in the other channel", func() bool { return len(indicatorPosts(api)) == 2 })

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: "100.000700"}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	eventually(t, "the indicator to move to the other thread", func() bool { return len(indicatorPosts(api)) == 3 })
	moved := indicatorPosts(api)[2]
	if moved.Channel != "C0ELSEWHERE" || moved.ThreadTS != "100.000700" {
		t.Errorf("moved indicator posted as %+v, want thread 100.000700 of C0ELSEWHERE", moved)
	}
}
