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

// An indicator already has a surface, and it is the one the owner is watching.
// A thread_ts passed alongside it is stale information from a call that could
// not know an indicator was running.
func TestProgressDoesNotMoveARunningIndicator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if _, err := b.Progress(ctx, ProgressRequest{Text: progressLabel, ThreadTS: "100.000200"}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	eventually(t, "the label to reach the channel", func() bool {
		return strings.Contains(lastIndicatorUpdate(api), progressLabel)
	})
	if got := indicatorPosts(api); len(got) != 1 {
		t.Errorf("indicator posts = %+v, want the running one left where it was", got)
	}
}

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

// The indicator must not go out with blocks on it. It is updated with text
// alone, and Slack keeps the blocks of a message an update does not mention,
// so a block would freeze the stopwatch at whatever it said when it was
// posted. Asserting the method, not just the text: the two differ only in
// whether the message carries a block, which the fake cannot show any other
// way.
func TestTheIndicatorPostsWithoutBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if got := indicatorPosts(api)[0]; !got.Plain {
		t.Errorf("indicator posted through Post, want PostPlain so a later text-only update replaces it: %+v", got)
	}

	// The contrast that gives the assertion meaning: an agent's reply does
	// take the rendered path.
	if _, err := b.Post(ctx, PostRequest{Text: "**done**"}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}
	replies := agentPosts(api)
	if len(replies) != 1 {
		t.Fatalf("agent posts = %d, want the one reply", len(replies))
	}
	if replies[0].Plain {
		t.Error("the reply went through PostPlain, want the rendered path")
	}
}
