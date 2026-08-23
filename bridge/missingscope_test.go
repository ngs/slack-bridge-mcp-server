package bridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/slack-go/slack"
)

// missingScope is what the Slack client hands back when the installed app was
// never granted a scope. The app it describes is the one already out there: a
// v0.2 install that has not been reinstalled since conversations outside the
// home channel existed.
func missingScope(method string) error {
	return fmt.Errorf("%s: %w", method, ErrMissingScope)
}

// captureLog collects what the bridge writes to the log for the duration of
// one test, so a "said it once" claim can be counted rather than assumed.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()

	buf := &bytes.Buffer{}
	out, flags := log.Writer(), log.Flags()
	log.SetOutput(buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(out)
		log.SetFlags(flags)
	})
	return buf
}

// degradationNotices counts the lines telling the operator that conversations
// outside the home channel are off.
func degradationNotices(logs *bytes.Buffer) int {
	return strings.Count(logs.String(), "conversations outside the home channel are off")
}

// The bug this guards against: an app installed before the mention feature
// existed has no channels:read, users.conversations fails, and the failure was
// fatal to the whole of slack_wait — so the owner's own home channel, which
// needs none of the new scopes, stopped delivering anything at all.
func TestAMissingScopeDoesNotStopTheHomeChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := mentionBridge(ctx, t)
	logs := captureLog(t)

	api.mu.Lock()
	api.joinedErr = missingScope("users.conversations")
	api.channelHistory[testChannel] = append(api.channelHistory[testChannel], ownerMsg("100.000200", "are you there?"))
	api.mu.Unlock()

	msgs := waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "100.000200" {
		t.Fatalf("Wait() returned %v, want the home channel message: it needs none of the scopes that are missing", texts(msgs))
	}

	// A reconnect runs catch-up again, and the home channel keeps working.
	api.mu.Lock()
	api.channelHistory[testChannel] = append(api.channelHistory[testChannel], ownerMsg("100.000300", "still here"))
	api.mu.Unlock()
	stream.events <- StreamEvent{Kind: StreamConnected}

	msgs = waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "100.000300" {
		t.Fatalf("Wait() after a reconnect returned %v, want the home channel still delivering", texts(msgs))
	}

	api.mu.Lock()
	scans := len(api.joinedCalls)
	api.mu.Unlock()
	if scans != 1 {
		t.Errorf("users.conversations was called %d times, want 1: the answer cannot change without a reinstall", scans)
	}
	if notices := degradationNotices(logs); notices != 1 {
		t.Errorf("the log carries %d degradation notices, want exactly 1", notices)
	}
	if !strings.Contains(logs.String(), "channels:read") {
		t.Errorf("the degradation notice does not name the scope to add:\n%s", logs.String())
	}
}

// The channel listing is not the only call the new scopes are needed for:
// reading a page of a channel that is not the home one wants a history scope
// the old installation may not have either.
func TestAMissingScopeReadingAnotherChannelIsNotFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the home cursor: %v", err)
	}
	// A cursor already recorded is what makes the scan a real search rather
	// than a first run that only seeds.
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatalf("seeding the mention cursor: %v", err)
	}

	api := &fakeAPI{
		botUserID: testBotUser,
		joined:    []string{testChannel, otherChannel},
		channelHistory: map[string][]candidate{
			testChannel: {ownerMsg("100.000100", "already answered"), ownerMsg("100.000200", "morning")},
		},
		channelHistoryErr: map[string]error{otherChannel: missingScope("conversations.history")},
	}

	logs := captureLog(t)
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	msgs := waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "100.000200" {
		t.Fatalf("Wait() returned %v, want the home channel message despite the scan being refused", texts(msgs))
	}
	if notices := degradationNotices(logs); notices != 1 {
		t.Errorf("the log carries %d degradation notices, want exactly 1", notices)
	}
}

// A conversation already open is walked with conversations.replies, which in a
// channel outside the home one needs a scope of its own. The mention in hand is
// still delivered; only the walk is given up.
func TestAMissingScopeReadingAThreadIsNotFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := mentionBridge(ctx, t)

	send(stream, otherChannel, "200.000100", "", mention("take a look"))
	if msgs := waitFor(ctx, t, b); len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the mention", texts(msgs))
	}

	logs := captureLog(t)
	api.mu.Lock()
	api.repliesErr = missingScope("conversations.replies")
	api.channelHistory[testChannel] = append(api.channelHistory[testChannel], ownerMsg("100.000200", "anything?"))
	api.mu.Unlock()
	stream.events <- StreamEvent{Kind: StreamConnected}

	msgs := waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "100.000200" {
		t.Fatalf("Wait() returned %v, want the home channel message: a thread the app cannot read is not a delivery failure", texts(msgs))
	}

	// The thread is still open, so a reply arriving live is relayed as before.
	send(stream, otherChannel, "200.000200", "200.000100", "and the rollback too")
	if msgs := waitFor(ctx, t, b); len(msgs) != 1 || msgs[0].TS != "200.000200" {
		t.Fatalf("Wait() returned %v, want the live thread reply: the stream needs no extra scope", texts(msgs))
	}
	if notices := degradationNotices(logs); notices != 1 {
		t.Errorf("the log carries %d degradation notices, want exactly 1", notices)
	}
}

// The degradation lasts the connection and no longer. Reinstalling the app is
// how the scopes arrive, and a reinstall means a new connection, so the next
// one has to try again rather than stay switched off.
func TestALaterConnectionRetriesTheScan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := mentionBridge(ctx, t)
	stateDir := b.cfg.StateDir

	api.mu.Lock()
	api.joinedErr = missingScope("users.conversations")
	api.mu.Unlock()

	waitFor(ctx, t, b)
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// A new process against the same state, with the app now reinstalled.
	cfg := testConfig(t)
	cfg.StateDir = stateDir
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	reinstalled := &fakeAPI{
		botUserID:      testBotUser,
		joined:         []string{testChannel, otherChannel},
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "already answered")}},
	}
	restarted := New(ctx, cfg, &fakeConnector{api: reinstalled, stream: newFakeStream()})
	t.Cleanup(func() { _ = restarted.Close() })

	waitFor(ctx, t, restarted)

	reinstalled.mu.Lock()
	scans := len(reinstalled.joinedCalls)
	reinstalled.mu.Unlock()
	if scans == 0 {
		t.Fatal("the new connection did not look for missed mentions; a reinstall would never take effect")
	}

	// The scan that ran seeded its cursor, which is what makes the next one a
	// search: proof it got as far as succeeding rather than being skipped.
	cursor, err := NewStore(stateDir).MentionCursor()
	if err != nil {
		t.Fatalf("reading the mention cursor: %v", err)
	}
	if cursor == "" {
		t.Error("the mention cursor is still unset, so the scan did not complete on the new connection")
	}
}

// Catch-up runs with the lock released, so a slow one can outlive the
// connection it belongs to. What it reports about that installation says
// nothing about the connection that replaced it — which, after a reinstall, is
// exactly the one whose scan must run.
func TestAStaleRefusalDoesNotDegradeTheNextConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)
	waitFor(ctx, t, b)

	b.mu.Lock()
	stale := b.connGeneration
	// The connection is replaced while a catch-up on the old one is still out.
	b.connGeneration++
	current := b.connGeneration
	b.mu.Unlock()

	logs := captureLog(t)
	b.degradeConversations(stale, missingScope("users.conversations"))

	if b.conversationsAreDegraded(current) {
		t.Error("a refusal from the previous connection switched off the new connection's scan")
	}
	if notices := degradationNotices(logs); notices != 0 {
		t.Errorf("the log carries %d degradation notices for a connection that is gone, want 0", notices)
	}
}

// replacingAPI stands in for a connection being replaced underneath a catch-up
// that is already running: the replacement is installed while the old API is
// still answering.
type replacingAPI struct {
	*fakeAPI
	b    *Bridge
	once bool
}

func (r *replacingAPI) History(ctx context.Context, req HistoryRequest) (HistoryPage, error) {
	if !r.once {
		r.once = true
		r.b.mu.Lock()
		r.b.connGeneration++
		r.b.mu.Unlock()
	}
	return r.fakeAPI.History(ctx, req)
}

// Catching up is what a new connection does first, and a drain belonging to the
// connection it replaced must not report that work as done on its behalf — the
// replacement's own scan is the one that finds what a reinstall made readable.
func TestAStaleDrainDoesNotClearTheNewConnectionsCatchUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := mentionBridge(ctx, t)
	waitFor(ctx, t, b)

	b.mu.Lock()
	b.api = &replacingAPI{fakeAPI: api, b: b}
	b.needCatchUp = true
	b.mu.Unlock()

	if _, err := b.drainCatchUp(ctx); err != nil {
		t.Fatalf("drainCatchUp() error = %v", err)
	}

	b.mu.Lock()
	pending := b.needCatchUp
	b.mu.Unlock()
	if !pending {
		t.Error("a drain from the previous connection marked the replacement as caught up; its first scan would be skipped")
	}
}

// The client is what decides a failure is a missing scope, and it has to tell
// that one apart from the failures worth retrying.
func TestMissingScopeIsRecognisedOnTheWire(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{"a scope the app was never granted", slack.SlackErrorResponse{Err: "missing_scope"}, true},
		{"a rate limit", slack.SlackErrorResponse{Err: "ratelimited"}, false},
		{"a channel the bot is not in", slack.SlackErrorResponse{Err: "not_in_channel"}, false},
		{"a transport failure", errors.New("connection reset"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := apiError("users.conversations", tt.err)
			if got := errors.Is(err, ErrMissingScope); got != tt.want {
				t.Errorf("errors.Is(%v, ErrMissingScope) = %v, want %v", err, got, tt.want)
			}
			if !strings.Contains(err.Error(), "users.conversations") {
				t.Errorf("error %q does not name the method that failed", err)
			}
		})
	}
}
