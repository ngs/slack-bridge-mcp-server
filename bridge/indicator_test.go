package bridge

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The indicator is deliberately slow to appear, so the tests drive it with
// durations short enough to observe without sleeping through a real grace
// period.
const (
	testGrace    = 20 * time.Millisecond
	testInterval = 20 * time.Millisecond
)

func (f *fakeAPI) snapshotPosts() []postCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]postCall(nil), f.posts...)
}

func (f *fakeAPI) snapshotUpdates() []updateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]updateCall(nil), f.updates...)
}

func (f *fakeAPI) snapshotDeletes() []deleteCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deleteCall(nil), f.deletes...)
}

// eventually polls because the indicator's Slack calls happen on its own
// goroutine: the tool call that starts or stops it returns first, by design.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// indicatorBridge returns a bridge whose next Wait hands over one message, with
// the indicator wound down to test speed.
func indicatorBridge(ctx context.Context, t *testing.T) (*Bridge, *fakeAPI, *fakeStream) {
	t.Helper()

	return indicatorBridgeWith(ctx, t, []candidate{
		ownerMsg("100.000100", "already answered"),
		ownerMsg("100.000200", "please look into this"),
	})
}

// indicatorBridgeWith is indicatorBridge over a chosen channel history, so a
// test can decide whether the message the agent is handed sits on the channel
// surface or inside a thread.
func indicatorBridgeWith(ctx context.Context, t *testing.T, history []candidate) (*Bridge, *fakeAPI, *fakeStream) {
	t.Helper()

	cfg := testConfig(t)
	cfg.IndicatorGrace = testGrace
	cfg.IndicatorInterval = testInterval
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: history, postTS: "100.000900"}

	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })
	return b, api, stream
}

// waitForMessages performs the Wait that hands messages to the agent, which is
// what starts the clock.
func waitForMessages(ctx context.Context, t *testing.T, b *Bridge) {
	t.Helper()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) == 0 {
		t.Fatal("Wait() returned no messages, so the indicator would never start")
	}
}

// indicatorPost finds the indicator among the fake's posts. The reply the agent
// sends is a post too, so they are told apart by their text.
func indicatorPosts(api *fakeAPI) []postCall {
	var found []postCall
	for _, p := range api.snapshotPosts() {
		if strings.HasPrefix(p.Text, "⏳") {
			found = append(found, p)
		}
	}
	return found
}

// Close runs as the process is about to exit, and the chat.delete that clears
// the indicator lives on a goroutine that exiting would kill. If Close did not
// wait for it, the owner would be left with a "⏳ Working…" message that never
// goes away.
func TestCloseWaitsForTheIndicatorToClearTheChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// No polling: by the time Close returns, the deletion has already
	// happened, which is the whole point of waiting for it.
	if got := api.snapshotDeletes(); len(got) != 1 {
		t.Errorf("deletes after Close() = %+v, want the indicator message already removed", got)
	}
}

// A reply that never reaches Slack has not answered anything, so the progress
// signal has to come back — and come back counting from when the agent was
// handed the work, not from the failed attempt.
func TestAFailedReplyPutsTheIndicatorBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to appear", func() bool { return len(indicatorPosts(api)) == 1 })

	started := b.indicatorStartedAt()

	api.mu.Lock()
	api.postErr = errors.New("channel_not_found")
	api.mu.Unlock()

	if _, err := b.Post(ctx, PostRequest{Text: "here you go", ThreadTS: ""}); err == nil {
		t.Fatal("Post() = nil error, want the failure surfaced")
	}

	eventually(t, "the indicator to come back", func() bool { return len(indicatorPosts(api)) == 2 })
	if got := b.indicatorStartedAt(); !got.Equal(started) {
		t.Errorf("restored indicator started at %v, want the original %v: the elapsed time must not reset", got, started)
	}
}

// Nothing was working, so nothing should start claiming to be. A failed reply
// from a session that never called slack_wait must not invent an indicator.
func TestAFailedReplyDoesNotInventAnIndicator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	api.mu.Lock()
	api.postErr = errors.New("channel_not_found")
	api.mu.Unlock()

	if _, err := b.Post(ctx, PostRequest{Text: "unprompted", ThreadTS: ""}); err == nil {
		t.Fatal("Post() = nil error, want the failure surfaced")
	}

	time.Sleep(4 * testGrace)
	if got := indicatorPosts(api); len(got) != 0 {
		t.Errorf("indicator posts = %+v with no work in flight, want none", got)
	}
}

// indicatorStartedAt reports the clock the running indicator is counting from.
func (b *Bridge) indicatorStartedAt() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.indicator == nil {
		return time.Time{}
	}
	return b.indicator.startedAt
}

// A stop is a decision even when it finds nothing to stop. A slow failing
// reply must not be able to overrule a later call that said "no indicator from
// here" by putting its own back afterwards.
func TestAStopInvalidatesAnInFlightRestore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to appear", func() bool { return len(indicatorPosts(api)) == 1 })

	// A reply retires the indicator and then takes its time with Slack.
	retired := b.stopIndicator()
	// Meanwhile another call decides there should be no indicator at all.
	b.stopIndicator()
	// The reply now fails and tries to put back what it retired.
	b.restoreIndicator(ctx, retired)

	time.Sleep(4 * testGrace)
	if got := indicatorPosts(api); len(got) != 1 {
		t.Errorf("indicator posts = %d, want the restore refused: something stopped the indicator after this one did", len(got))
	}
}

// The common case is a reply within seconds. Nothing should reach the channel
// then, or the owner gets a "working…" note that is gone before it can be read.
func TestNoIndicatorForRepliesFasterThanTheGracePeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)

	if _, err := b.Post(ctx, PostRequest{Text: "here you go", ThreadTS: ""}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	// Well past the grace period: if the indicator were going to appear, it
	// would have by now.
	time.Sleep(5 * testGrace)

	if got := indicatorPosts(api); len(got) != 0 {
		t.Errorf("indicator posted %+v, want nothing for a reply inside the grace period", got)
	}
	if got := api.snapshotUpdates(); len(got) != 0 {
		t.Errorf("indicator updated %+v, want nothing", got)
	}
	if got := api.snapshotDeletes(); len(got) != 0 {
		t.Errorf("indicator deleted %+v, want nothing", got)
	}
}

// The full life of a slow turn: appear once, tick with a growing elapsed time,
// and disappear when the answer arrives.
func TestIndicatorPostsTicksAndIsDeletedOnReply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)

	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })
	posted := indicatorPosts(api)[0]
	if posted.Channel != testChannel || posted.ThreadTS != "" {
		t.Errorf("indicator posted as %+v, want a plain message on the bound channel", posted)
	}
	if !strings.Contains(posted.Text, "Working") {
		t.Errorf("indicator text = %q, want it to say what is happening", posted.Text)
	}

	// The elapsed time is only rendered to the second, so the first tick that
	// crosses a second boundary is the one that must show a different value.
	eventually(t, "the elapsed time to advance", func() bool {
		for _, u := range api.snapshotUpdates() {
			if u.TS == "100.000900" && u.Text != posted.Text {
				return true
			}
		}
		return false
	})

	if _, err := b.Post(ctx, PostRequest{Text: "done", ThreadTS: ""}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	eventually(t, "the indicator to be deleted", func() bool {
		for _, d := range api.snapshotDeletes() {
			if d.Channel == testChannel && d.TS == "100.000900" {
				return true
			}
		}
		return false
	})

	// Only ever one indicator message, however long the turn ran.
	if got := indicatorPosts(api); len(got) != 1 {
		t.Errorf("indicator posted %d times, want exactly 1", len(got))
	}
}

// threadedBridge hands the agent one message sent inside a thread, which is
// where a conversation with the owner usually lives: they reply under the
// message they are talking about rather than starting a new one.
func threadedBridge(ctx context.Context, t *testing.T) (*Bridge, *fakeAPI, *fakeStream) {
	t.Helper()

	b, api, stream := indicatorBridgeWith(ctx, t, []candidate{ownerMsg("100.000100", "already answered")})
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "100.000300", ThreadTS: "100.000200", User: testOwner, Text: "and what about this one",
	}}
	return b, api, stream
}

// The indicator has to appear where the owner is looking. A turn that started
// with a message inside a thread is answered in that thread, so a "⏳ Working…"
// on the channel surface is noise in one place and silence in the other.
func TestIndicatorPostsIntoTheThreadTheTurnBelongsTo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadedBridge(ctx, t)
	waitForMessages(ctx, t, b)

	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })
	if got := indicatorPosts(api)[0]; got.ThreadTS != "100.000200" {
		t.Errorf("indicator posted as %+v, want it in thread 100.000200 with the conversation", got)
	}
}

// A batch can span more than one thread — a reconnect delivers everything
// missed at once. The owner is waiting where they spoke last, so the newest
// message decides, and a surface message keeps the indicator on the surface.
func TestIndicatorFollowsTheNewestMessageInTheBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A catch-up after a reconnect: a reply the owner left in a thread, and a
	// later one out on the channel surface, both arriving in one batch.
	b, api, _ := indicatorBridgeWith(ctx, t, []candidate{
		ownerMsg("100.000100", "already answered"),
		{Channel: testChannel, User: "U0COLLEAGUE", Text: "a thread to reply in", TS: "100.000200", LatestReply: "100.000300"},
		ownerMsg("100.000400", "then out here instead"),
	})
	api.replies = []candidate{
		{Channel: testChannel, User: testOwner, Text: "asked in a thread", TS: "100.000300", ThreadTS: "100.000200"},
	}

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("Wait() returned %v, want the thread reply and the surface message in one batch", texts(result.Messages))
	}

	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })
	if got := indicatorPosts(api)[0]; got.ThreadTS != "" {
		t.Errorf("indicator posted as %+v, want it on the channel surface where the newest message was", got)
	}
}

// A restore is the same turn resuming, so it goes back to the thread it was in
// — the failed reply changed nothing about where the owner is waiting.
func TestARestoredIndicatorGoesBackToItsThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadedBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to appear", func() bool { return len(indicatorPosts(api)) == 1 })

	api.mu.Lock()
	api.postErr = errors.New("channel_not_found")
	api.mu.Unlock()

	if _, err := b.Post(ctx, PostRequest{Text: "here you go", ThreadTS: "100.000200"}); err == nil {
		t.Fatal("Post() = nil error, want the failure surfaced")
	}

	eventually(t, "the indicator to come back", func() bool { return len(indicatorPosts(api)) == 2 })
	if got := indicatorPosts(api)[1]; got.ThreadTS != "100.000200" {
		t.Errorf("restored indicator posted as %+v, want it back in thread 100.000200", got)
	}
}

// Going back to waiting without replying still ends the turn, so the indicator
// must not be left counting in the channel.
func TestNextWaitRemovesTheIndicator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)

	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if _, err := b.Wait(ctx, 10*time.Millisecond); err != nil {
		t.Fatalf("second Wait() error = %v", err)
	}

	eventually(t, "the indicator to be deleted", func() bool { return len(api.snapshotDeletes()) == 1 })
}

// Two turns in a row, with the first indicator's chat.delete still in flight
// when the second turn starts. The replacement has to hold its post until the
// old message is actually gone, or the owner briefly sees two of them — and
// chat.delete is allowed longer than the shortest grace period, so this is not
// a theoretical window.
func TestReplacementWaitsForTheOutgoingIndicatorToBeDeleted(t *testing.T) {
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

	// A second turn begins before the first indicator has finished cleaning up.
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300", User: testOwner, Text: "and another thing"}}
	waitForMessages(ctx, t, b)

	eventually(t, "the outgoing indicator to start deleting", func() bool { return len(api.snapshotDeletes()) == 1 })

	// The replacement's grace period has expired several times over by now;
	// only the pending deletion should be holding it back.
	time.Sleep(6 * testGrace)
	if got := indicatorPosts(api); len(got) != 1 {
		t.Fatalf("indicator posts = %d while the previous message was still being deleted, want 1", len(got))
	}

	release()
	eventually(t, "the replacement to post once the channel is clear", func() bool {
		return len(indicatorPosts(api)) == 2
	})
}

// slack_ack means "seen, still working", which is precisely when the owner
// wants the clock to keep running.
func TestReactDoesNotStopTheIndicator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)

	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if err := b.React(ctx, ReactRequest{TS: "100.000200", Emoji: "eyes"}); err != nil {
		t.Fatalf("React() error = %v", err)
	}

	// The ticker must still be running after the ack.
	eventually(t, "the indicator to keep ticking", func() bool { return len(api.snapshotUpdates()) > 0 })
	if got := api.snapshotDeletes(); len(got) != 0 {
		t.Errorf("indicator deleted %+v after an ack, want it left running", got)
	}
}

func TestIndicatorCanBeTurnedOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	b.cfg.IndicatorDisabled = true

	waitForMessages(ctx, t, b)
	time.Sleep(5 * testGrace)

	if got := indicatorPosts(api); len(got) != 0 {
		t.Errorf("indicator posted %+v with the feature off, want nothing", got)
	}
}

// The indicator is a convenience; a Slack failure in it must never cost the
// owner a reply.
func TestIndicatorFailuresDoNotReachTheTools(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	api.mu.Lock()
	api.updateErr = errors.New("slack said no")
	api.deleteErr = errors.New("slack said no again")
	api.mu.Unlock()

	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to try an update", func() bool { return len(api.snapshotUpdates()) > 1 })

	if _, err := b.Post(ctx, PostRequest{Text: "the answer", ThreadTS: ""}); err != nil {
		t.Fatalf("Post() error = %v, want the reply to succeed despite the indicator failing", err)
	}
	if _, err := b.Wait(ctx, 10*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v, want it unaffected by the indicator", err)
	}
}

// A failed chat.postMessage leaves nothing to update or delete, so the
// indicator gives up quietly rather than hammering Slack.
func TestIndicatorGivesUpWhenItCannotPost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	api.mu.Lock()
	api.postErr = errors.New("slack is down")
	api.mu.Unlock()

	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to attempt its post", func() bool { return len(indicatorPosts(api)) == 1 })
	time.Sleep(3 * testInterval)

	if got := api.snapshotUpdates(); len(got) != 0 {
		t.Errorf("indicator updated %+v after failing to post, want nothing", got)
	}
	if got := api.snapshotDeletes(); len(got) != 0 {
		t.Errorf("indicator deleted %+v after failing to post, want nothing", got)
	}

	api.mu.Lock()
	api.postErr = nil
	api.mu.Unlock()
	if _, err := b.Post(ctx, PostRequest{Text: "the answer", ThreadTS: ""}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}
}

func TestFormatElapsed(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"},
		{-time.Second, "0s"},
		{45 * time.Second, "45s"},
		{59*time.Second + 999*time.Millisecond, "59s"},
		{time.Minute, "1m 00s"},
		{65 * time.Second, "1m 05s"},
		{12*time.Minute + 30*time.Second, "12m 30s"},
		{time.Hour + 2*time.Minute + 45*time.Second, "1h 02m"},
		{25 * time.Hour, "25h 00m"},
	}

	for _, tt := range tests {
		if got := formatElapsed(tt.d); got != tt.want {
			t.Errorf("formatElapsed(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestIndicatorSettingsFromTheEnvironment(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		cfg := LoadConfig()
		if cfg.IndicatorDisabled {
			t.Error("the indicator is disabled by default, want it on")
		}
		grace, interval := cfg.indicatorTimings()
		if grace != DefaultIndicatorGrace || interval != DefaultIndicatorInterval {
			t.Errorf("timings = %v/%v, want %v/%v", grace, interval, DefaultIndicatorGrace, DefaultIndicatorInterval)
		}
	})

	t.Run("off", func(t *testing.T) {
		t.Setenv(EnvIndicator, "off")
		if !LoadConfig().IndicatorDisabled {
			t.Error("SLACK_BRIDGE_INDICATOR=off did not disable the indicator")
		}
	})

	// Out-of-range and nonsense values must never keep a session from
	// starting; they fall back to something workable.
	t.Run("clamped and tolerant", func(t *testing.T) {
		t.Setenv(EnvIndicatorGrace, "1")
		t.Setenv(EnvIndicatorInterval, "600")
		grace, interval := LoadConfig().indicatorTimings()
		if grace != MinIndicatorGrace || interval != MaxIndicatorInterval {
			t.Errorf("timings = %v/%v, want them clamped to %v/%v", grace, interval, MinIndicatorGrace, MaxIndicatorInterval)
		}

		t.Setenv(EnvIndicatorGrace, "soon")
		t.Setenv(EnvIndicatorInterval, "-5")
		grace, interval = LoadConfig().indicatorTimings()
		if grace != DefaultIndicatorGrace || interval != DefaultIndicatorInterval {
			t.Errorf("timings = %v/%v, want the defaults for unusable values", grace, interval)
		}
	})

	// A second count large enough to overflow time.Duration once multiplied by
	// a billion must still land on the maximum, not wrap into a negative
	// duration and come back as the minimum.
	t.Run("absurdly large values do not wrap", func(t *testing.T) {
		if strconv.IntSize < 64 {
			t.Skip("int is too narrow here for the value to parse at all")
		}

		huge := strconv.Itoa(1 << 62)
		t.Setenv(EnvIndicatorGrace, huge)
		t.Setenv(EnvIndicatorInterval, huge)

		grace, interval := LoadConfig().indicatorTimings()
		if grace != MaxIndicatorGrace || interval != MaxIndicatorInterval {
			t.Errorf("timings = %v/%v, want them clamped to %v/%v", grace, interval, MaxIndicatorGrace, MaxIndicatorInterval)
		}
	})
}
