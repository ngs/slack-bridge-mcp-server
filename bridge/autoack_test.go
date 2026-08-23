package bridge

import (
	"context"
	"errors"
	"testing"
	"time"
)

// autoAckBridge returns a bridge whose next Wait hands over two messages.
func autoAckBridge(ctx context.Context, t *testing.T, configure func(*Config)) (*Bridge, *fakeAPI) {
	t.Helper()

	cfg := testConfig(t)
	// The indicator is not what these tests are about, and leaving it on would
	// put unrelated posts and deletes through the same fake.
	cfg.IndicatorDisabled = true
	if configure != nil {
		configure(&cfg)
	}
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: []candidate{
		ownerMsg("100.000100", "already answered"),
		ownerMsg("100.000200", "first"),
		ownerMsg("100.000300", "second"),
	}}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })
	return b, api
}

func (f *fakeAPI) snapshotReactions() []reactionCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]reactionCall(nil), f.reactions...)
}

// The point of the feature: the owner's phone shows the message was received
// at the moment it was received, not whenever the model gets around to saying
// so.
func TestWaitMarksEveryDeliveredMessageAsReceived(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := autoAckBridge(ctx, t, nil)

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("Wait() returned %d messages, want 2", len(result.Messages))
	}

	eventually(t, "both messages to be marked as received", func() bool {
		return len(api.snapshotReactions()) == 2
	})
	want := []reactionCall{
		{Channel: testChannel, TS: "100.000200", Emoji: DefaultAutoAckEmoji},
		{Channel: testChannel, TS: "100.000300", Emoji: DefaultAutoAckEmoji},
	}
	got := api.snapshotReactions()
	for i, w := range want {
		if got[i] != w {
			t.Errorf("reaction %d = %+v, want %+v", i, got[i], w)
		}
	}
}

// A timed-out wait delivered nothing, so there is nothing to mark.
func TestWaitMarksNothingWhenItDeliversNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}
	api := &fakeAPI{}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	if _, err := b.Wait(ctx, 20*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	if got := api.snapshotReactions(); len(got) != 0 {
		t.Errorf("reactions = %+v after an empty wait, want none", got)
	}
}

func TestAutoAckHonoursTheConfiguredEmoji(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := autoAckBridge(ctx, t, func(cfg *Config) { cfg.AutoAckEmoji = "inbox_tray" })

	if _, err := b.Wait(ctx, MaxWaitTimeout); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	eventually(t, "the configured emoji to be used", func() bool {
		got := api.snapshotReactions()
		return len(got) == 2 && got[0].Emoji == "inbox_tray"
	})
}

// People write a reaction the way they type it in Slack. reactions.add wants
// the bare name, so the colons come off before the value is ever used.
func TestAutoAckEmojiSettingIsNormalised(t *testing.T) {
	tests := map[string]string{
		":thumbsup:":  "thumbsup",
		"  :eyes:  ":  "eyes",
		"white_check": "white_check",
		"":            "",
	}
	for input, want := range tests {
		if got := emojiName(input); got != want {
			t.Errorf("emojiName(%q) = %q, want %q", input, got, want)
		}
	}

	// An unset value still has to reach Slack as something valid.
	if got := (Config{}).autoAckEmoji(); got != DefaultAutoAckEmoji {
		t.Errorf("autoAckEmoji() on a zero Config = %q, want %q", got, DefaultAutoAckEmoji)
	}

	t.Setenv(EnvAutoAckEmoji, ":thumbsup:")
	t.Setenv(EnvAutoAck, "off")
	cfg := LoadConfig()
	if cfg.AutoAckEmoji != "thumbsup" {
		t.Errorf("AutoAckEmoji = %q, want the colons trimmed", cfg.AutoAckEmoji)
	}
	if !cfg.AutoAckDisabled {
		t.Error("AutoAckDisabled = false with SLACK_BRIDGE_AUTO_ACK=off, want it disabled")
	}
}

func TestAutoAckCanBeTurnedOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := autoAckBridge(ctx, t, func(cfg *Config) { cfg.AutoAckDisabled = true })

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("Wait() returned %d messages, want 2", len(result.Messages))
	}

	time.Sleep(20 * time.Millisecond)
	if got := api.snapshotReactions(); len(got) != 0 {
		t.Errorf("reactions = %+v with the receipt reaction off, want none", got)
	}
}

// The receipt is a courtesy. Slack refusing it must not cost the owner the
// messages, which is the whole reason it happens off to the side.
func TestAutoAckFailuresNeverReachTheCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for name, reactErr := range map[string]error{
		"already reacted": ErrAlreadyReacted,
		"slack is down":   errors.New("reactions.add: rate_limited"),
	} {
		t.Run(name, func(t *testing.T) {
			b, api := autoAckBridge(ctx, t, nil)
			api.mu.Lock()
			api.reactErr = reactErr
			api.mu.Unlock()

			result, err := b.Wait(ctx, MaxWaitTimeout)
			if err != nil {
				t.Fatalf("Wait() error = %v, want the messages delivered regardless", err)
			}
			if len(result.Messages) != 2 || result.TimedOut {
				t.Errorf("Wait() = %+v, want both messages and no timeout", result)
			}
			eventually(t, "the reaction to be attempted", func() bool {
				return len(api.snapshotReactions()) == 2
			})
		})
	}
}

// slack_ack promises the message is marked, not that this call is what marked
// it — and the automatic receipt will often have got there first.
func TestAckTreatsAnExistingReactionAsSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := autoAckBridge(ctx, t, func(cfg *Config) { cfg.AutoAckDisabled = true })
	api.mu.Lock()
	api.reactErr = ErrAlreadyReacted
	api.mu.Unlock()

	if err := b.React(ctx, "100.000200", "eyes"); err != nil {
		t.Errorf("React() = %v, want an existing reaction treated as success", err)
	}

	api.mu.Lock()
	api.reactErr = errors.New("channel_not_found")
	api.mu.Unlock()

	if err := b.React(ctx, "100.000200", "eyes"); err == nil {
		t.Error("React() = nil error on a real failure, want it surfaced")
	}
}
