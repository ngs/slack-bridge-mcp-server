package bridge

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAPI stands in for the Slack Web API. Pages are served oldest-last, the
// way conversations.history does.
type fakeAPI struct {
	mu sync.Mutex

	// history is every message in the channel, oldest first.
	history []candidate
	// historyErr, when set, fails the next History call.
	historyErr error

	historyCalls []HistoryRequest
	replyCalls   []RepliesRequest
	// replies is the thread conversations.replies serves, oldest first.
	replies []candidate
	// repliesPageSize, when set, makes the fake page its replies the way
	// Slack does, handing back a cursor.
	repliesPageSize int
	repliesErr      error
	// names is what users.info resolves, and nameLookups counts the calls so a
	// test can see the cache working.
	names       map[string]string
	nameErr     error
	nameLookups int
	posts       []postCall
	questions   []questionCall
	reactions   []reactionCall
	updates     []updateCall
	resolutions []updateCall
	deletes     []deleteCall
	postTS      string
	questionTS  string
	// beforeQuestionReturns runs inside PostQuestion, after Slack would have
	// created the message but before the caller learns its ts.
	beforeQuestionReturns func()
	postErr               error
	questionErr           error
	reactErr              error
	updateErr             error
	resolveErr            error
	deleteErr             error
	// deleteGate, when set, holds Delete open until the test closes it, which
	// is how a slow chat.delete is simulated.
	deleteGate chan struct{}
}

type postCall struct{ Channel, ThreadTS, Text string }
type questionCall struct {
	Channel  string
	ThreadTS string
	Question Question
}
type reactionCall struct{ Channel, TS, Emoji string }
type updateCall struct{ Channel, TS, Text string }
type deleteCall struct{ Channel, TS string }

func (f *fakeAPI) History(_ context.Context, req HistoryRequest) (HistoryPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.historyCalls = append(f.historyCalls, req)
	if f.historyErr != nil {
		return HistoryPage{}, f.historyErr
	}

	var matched []candidate
	for _, c := range f.history {
		if req.Oldest != "" && !tsLess(req.Oldest, c.TS) {
			continue
		}
		// Both bounds are exclusive, the way Slack behaves with `inclusive`
		// left off.
		if req.Latest != "" && !tsLess(c.TS, req.Latest) {
			continue
		}
		matched = append(matched, c)
	}

	// Slack returns newest first, and Limit counts from the newest end.
	reversed := make([]candidate, 0, len(matched))
	for i := len(matched) - 1; i >= 0; i-- {
		reversed = append(reversed, matched[i])
	}

	hasMore := false
	if req.Limit > 0 && len(reversed) > req.Limit {
		reversed = reversed[:req.Limit]
		hasMore = true
	}
	return HistoryPage{Messages: reversed, HasMore: hasMore}, nil
}

func (f *fakeAPI) Replies(_ context.Context, req RepliesRequest) (HistoryPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.replyCalls = append(f.replyCalls, req)
	if f.repliesErr != nil {
		return HistoryPage{}, f.repliesErr
	}

	// Slack returns the parent plus the replies belonging to that thread,
	// oldest first, honouring the same exclusive bounds as history.
	matched := make([]candidate, 0, len(f.replies))
	for _, c := range f.replies {
		if req.ThreadTS != "" && c.ThreadTS != req.ThreadTS && c.TS != req.ThreadTS {
			continue
		}
		if req.Oldest != "" && !tsLess(req.Oldest, c.TS) {
			continue
		}
		if req.Latest != "" && !tsLess(c.TS, req.Latest) {
			continue
		}
		matched = append(matched, c)
	}

	// Slack pages long threads. The cursor here is simply how many replies the
	// caller has already been given.
	if size := f.repliesPageSize; size > 0 {
		start := 0
		if req.Cursor != "" {
			var err error
			if start, err = strconv.Atoi(req.Cursor); err != nil {
				return HistoryPage{}, err
			}
		}
		if start > len(matched) {
			start = len(matched)
		}
		end := min(start+size, len(matched))

		page := HistoryPage{Messages: matched[start:end]}
		if end < len(matched) {
			page.NextCursor = strconv.Itoa(end)
			page.HasMore = true
		}
		return page, nil
	}
	return HistoryPage{Messages: matched}, nil
}

func (f *fakeAPI) UserName(_ context.Context, userID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nameLookups++
	if f.nameErr != nil {
		return "", f.nameErr
	}
	return f.names[userID], nil
}

func (f *fakeAPI) Post(_ context.Context, channel, threadTS, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.posts = append(f.posts, postCall{Channel: channel, ThreadTS: threadTS, Text: text})
	if f.postErr != nil {
		return "", f.postErr
	}
	return f.postTS, nil
}

func (f *fakeAPI) PostQuestion(_ context.Context, channel, threadTS string, q Question) (string, error) {
	f.mu.Lock()
	f.questions = append(f.questions, questionCall{Channel: channel, ThreadTS: threadTS, Question: q})
	hook, ts, err := f.beforeQuestionReturns, f.questionTS, f.questionErr
	f.mu.Unlock()

	// The message exists in Slack before this call returns, so a test can use
	// the hook to click on it while the caller is still waiting.
	if hook != nil {
		hook()
	}
	if err != nil {
		return "", err
	}
	return ts, nil
}

func (f *fakeAPI) ResolveQuestion(_ context.Context, channel, ts, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.resolutions = append(f.resolutions, updateCall{Channel: channel, TS: ts, Text: text})
	return f.resolveErr
}

func (f *fakeAPI) React(_ context.Context, channel, ts, emoji string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.reactions = append(f.reactions, reactionCall{Channel: channel, TS: ts, Emoji: emoji})
	return f.reactErr
}

func (f *fakeAPI) Update(_ context.Context, channel, ts, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.updates = append(f.updates, updateCall{Channel: channel, TS: ts, Text: text})
	return f.updateErr
}

// Delete records the call before waiting on deleteGate, so a test can hold a
// deletion open and still see that it started.
func (f *fakeAPI) Delete(_ context.Context, channel, ts string) error {
	f.mu.Lock()
	f.deletes = append(f.deletes, deleteCall{Channel: channel, TS: ts})
	gate, err := f.deleteGate, f.deleteErr
	f.mu.Unlock()

	if gate != nil {
		<-gate
	}
	return err
}

func (f *fakeAPI) calls() []HistoryRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]HistoryRequest(nil), f.historyCalls...)
}

// fakeStream is a hand-driven live event stream.
type fakeStream struct {
	events       chan StreamEvent
	interactions chan Interaction
}

func newFakeStream() *fakeStream {
	return &fakeStream{
		events:       make(chan StreamEvent, 16),
		interactions: make(chan Interaction, 16),
	}
}

func (s *fakeStream) Events() <-chan StreamEvent { return s.events }

func (s *fakeStream) Interactions() <-chan Interaction { return s.interactions }

// fakeConnector hands out a fixed API and stream, and records how often it was
// asked to connect.
type fakeConnector struct {
	api    *fakeAPI
	stream *fakeStream
	err    error

	mu    sync.Mutex
	calls int
}

func (c *fakeConnector) Connect(context.Context, Config) (API, Stream, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()

	if c.err != nil {
		return nil, nil, c.err
	}
	return c.api, c.stream, nil
}

func (c *fakeConnector) connectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		BotToken: "xoxb-test",
		AppToken: "xapp-test",
		Channel:  testChannel,
		Owner:    testOwner,
		StateDir: t.TempDir(),
	}
}

func ownerMsg(ts, text string) candidate {
	return candidate{Channel: testChannel, User: testOwner, Text: text, TS: ts}
}

func texts(msgs []Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Text)
	}
	return out
}

// A session that starts against a channel it has never read should join the
// conversation, not replay it. Otherwise the first slack_wait of a new install
// dumps the channel's whole backlog into the model's context.
func TestFirstEverWaitSeedsTheCursorInsteadOfReplaying(t *testing.T) {
	api := &fakeAPI{history: []candidate{
		ownerMsg("100.000100", "ancient"),
		ownerMsg("100.000200", "old"),
	}}
	connector := &fakeConnector{api: api, stream: newFakeStream()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, testConfig(t), connector)
	defer func() { _ = b.Close() }()

	result, err := b.Wait(ctx, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if !result.TimedOut || len(result.Messages) != 0 {
		t.Fatalf("Wait() = %+v, want a timeout with no messages on a channel never read before", result)
	}
	if got := b.Status().LastTS; got != "100.000200" {
		t.Errorf("last_ts = %q, want the newest existing message so the next wait starts from there", got)
	}
}

// The whole point of catching up over Slack's own history: the machine slept,
// the session restarted, and the messages sent in between exist only in Slack.
func TestFirstWaitReturnsTheBacklogMissedWhileDown(t *testing.T) {
	cfg := testConfig(t)

	// A previous session got as far as 100.000100.
	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: []candidate{
		ownerMsg("100.000100", "already answered"),
		ownerMsg("100.000200", "missed one"),
		{Channel: testChannel, User: "U0SOMEONE", Text: "not the owner", TS: "100.000250"},
		ownerMsg("100.000300", "missed two"),
	}}
	connector := &fakeConnector{api: api, stream: newFakeStream()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, connector)
	defer func() { _ = b.Close() }()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.TimedOut {
		t.Fatal("Wait() timed out, want the backlog returned immediately")
	}

	want := []string{"missed one", "missed two"}
	if got := texts(result.Messages); !reflect.DeepEqual(got, want) {
		t.Errorf("Wait() messages = %v, want %v (oldest first, owner only)", got, want)
	}

	// The cursor must move only once the messages are in the caller's hands.
	if got := b.Status().LastTS; got != "100.000300" {
		t.Errorf("last_ts = %q, want 100.000300", got)
	}
	if got, _ := store.LastTS(testChannel); got != "100.000300" {
		t.Errorf("persisted last_ts = %q, want 100.000300 so a restart does not replay", got)
	}

	// Nothing new has happened, so the next wait should block rather than
	// hand the same messages over again.
	second, err := b.Wait(ctx, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("second Wait() error = %v", err)
	}
	if !second.TimedOut || len(second.Messages) != 0 {
		t.Errorf("second Wait() = %+v, want a timeout; the backlog was already delivered", second)
	}
}

func TestWaitReturnsLiveMessages(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: []candidate{ownerMsg("100.000100", "already answered")}}
	stream := newFakeStream()
	connector := &fakeConnector{api: api, stream: stream}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, connector)
	defer func() { _ = b.Close() }()

	go func() {
		time.Sleep(20 * time.Millisecond)
		stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000200", User: testOwner, Text: "live"}}
	}()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"live"}) {
		t.Fatalf("Wait() messages = %v, want [live]", got)
	}
	if b.Status().LastTS != "100.000200" {
		t.Errorf("last_ts = %q, want 100.000200", b.Status().LastTS)
	}
}

// A reconnect after sleep is the case the design is built around: the socket
// comes back, and whatever it missed has to be pulled from history and merged
// with whatever the socket did deliver, without duplicates.
func TestReconnectMergesHistoryWithLiveWithoutDuplicates(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: []candidate{
		ownerMsg("100.000100", "already answered"),
		ownerMsg("100.000200", "sent while asleep"),
		ownerMsg("100.000300", "also seen live"),
	}}
	stream := newFakeStream()
	connector := &fakeConnector{api: api, stream: stream}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, connector)
	defer func() { _ = b.Close() }()

	// The socket delivers the newest message and then reports the reconnect,
	// so both the live copy and the history copy of 100.000300 are in play.
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300", User: testOwner, Text: "also seen live"}}
	stream.events <- StreamEvent{Kind: StreamConnected}

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	want := []string{"sent while asleep", "also seen live"}
	if got := texts(result.Messages); !reflect.DeepEqual(got, want) {
		t.Errorf("Wait() messages = %v, want %v exactly once each, oldest first", got, want)
	}
}

// Overflowing the live buffer must not lose a message; it degrades to the same
// history catch-up a reconnect triggers.
func TestDroppedLiveEventTriggersCatchUp(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: []candidate{
		ownerMsg("100.000100", "already answered"),
		ownerMsg("100.000200", "recovered from history"),
	}}
	stream := newFakeStream()
	connector := &fakeConnector{api: api, stream: stream}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, connector)
	defer func() { _ = b.Close() }()

	// Drain the initial catch-up so the dropped event is the only trigger left.
	if _, err := b.Wait(ctx, MaxWaitTimeout); err != nil {
		t.Fatalf("initial Wait() error = %v", err)
	}
	api.mu.Lock()
	api.history = append(api.history, ownerMsg("100.000300", "the dropped one"))
	api.mu.Unlock()

	stream.events <- StreamEvent{Kind: StreamDropped}

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"the dropped one"}) {
		t.Errorf("Wait() messages = %v, want the dropped message recovered from history", got)
	}
}

func TestWaitTimesOutWithAnEmptyArray(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	connector := &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, connector)
	defer func() { _ = b.Close() }()

	result, err := b.Wait(ctx, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if !result.TimedOut {
		t.Error("Wait() TimedOut = false, want true")
	}
	// A nil slice marshals to null; the tool contract says an empty array.
	if result.Messages == nil || len(result.Messages) != 0 {
		t.Errorf("Wait() Messages = %#v, want an empty non-nil slice", result.Messages)
	}
}

// Connecting on the first slack_wait rather than at startup is what lets the
// server sit in every project's .mcp.json without opening a socket.
func TestConnectIsLazyAndHappensOnce(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	connector := &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, connector)
	defer func() { _ = b.Close() }()

	if got := connector.connectCount(); got != 0 {
		t.Fatalf("connector called %d times before any tool call, want 0", got)
	}
	if b.Status().Connected {
		t.Error("Status() reports connected before the first slack_wait")
	}

	for range 3 {
		if _, err := b.Wait(ctx, 10*time.Millisecond); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	}

	if got := connector.connectCount(); got != 1 {
		t.Errorf("connector called %d times across three waits, want 1", got)
	}
	if !b.Status().Connected {
		t.Error("Status() reports disconnected after a successful wait")
	}
}

func TestToolsReportMissingConfigurationByName(t *testing.T) {
	ctx := context.Background()
	cfg := Config{BotToken: "xoxb-test", StateDir: t.TempDir()}
	b := New(ctx, cfg, &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	_, waitErr := b.Wait(ctx, MinWaitTimeout)
	_, postErr := b.Post(ctx, "hello", "")
	reactErr := b.React(ctx, "100.000100", "eyes")

	for name, err := range map[string]error{"Wait": waitErr, "Post": postErr, "React": reactErr} {
		if err == nil {
			t.Errorf("%s() = nil error, want the missing variables named", name)
			continue
		}
		for _, v := range []string{EnvAppToken, EnvChannel, EnvOwner} {
			if !strings.Contains(err.Error(), v) {
				t.Errorf("%s() error = %q; want it to name %s", name, err, v)
			}
		}
		if strings.Contains(err.Error(), EnvBotToken) {
			t.Errorf("%s() error = %q; %s is set and should not be listed", name, err, EnvBotToken)
		}
	}

	// slack_status is the tool an operator reaches for when nothing works, so
	// it has to answer without Slack.
	status := b.Status()
	if status.Connected {
		t.Error("Status() Connected = true on an unconfigured bridge")
	}
	if status.ConfigError == "" {
		t.Error("Status() ConfigError is empty, want the missing variables named")
	}
}

func TestPostAndReact(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{postTS: "100.000900"}
	b := New(context.Background(), cfg, &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	ts, err := b.Post(context.Background(), "hello from the agent", "100.000100")
	if err != nil {
		t.Fatalf("Post() error = %v", err)
	}
	if ts != "100.000900" {
		t.Errorf("Post() = %q, want the posted ts 100.000900", ts)
	}
	if len(api.posts) != 1 || api.posts[0] != (postCall{Channel: testChannel, ThreadTS: "100.000100", Text: "hello from the agent"}) {
		t.Errorf("Post() sent %+v, want it addressed to the bound channel and thread", api.posts)
	}

	if _, err := b.Post(context.Background(), "", ""); err == nil {
		t.Error("Post() with empty text = nil error, want a validation error")
	}

	if err := b.React(context.Background(), "100.000100", ""); err != nil {
		t.Fatalf("React() error = %v", err)
	}
	// The default matters: slack_ack is meant to be callable with just a ts.
	if len(api.reactions) != 1 || api.reactions[0] != (reactionCall{Channel: testChannel, TS: "100.000100", Emoji: "eyes"}) {
		t.Errorf("React() sent %+v, want eyes on the bound channel", api.reactions)
	}

	if err := b.React(context.Background(), "", "eyes"); err == nil {
		t.Error("React() with no ts = nil error, want a validation error")
	}
}

// A history failure must leave the cursor alone, so the messages are fetched
// again next time instead of being skipped.
func TestCatchUpFailureDoesNotAdvanceTheCursor(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{
		history:    []candidate{ownerMsg("100.000200", "unread")},
		historyErr: errors.New("slack is down"),
	}
	b := New(context.Background(), cfg, &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	if _, err := b.Wait(context.Background(), MinWaitTimeout); err == nil {
		t.Fatal("Wait() = nil error, want the history failure surfaced")
	}
	if got := b.Status().LastTS; got != "100.000100" {
		t.Errorf("last_ts = %q after a failed catch-up, want it unchanged at 100.000100", got)
	}

	api.mu.Lock()
	api.historyErr = nil
	api.mu.Unlock()

	result, err := b.Wait(context.Background(), MinWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"unread"}) {
		t.Errorf("Wait() messages = %v, want the message recovered on the retry", got)
	}
}

func TestCatchUpAsksSlackForEverythingAfterTheCursor(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: []candidate{ownerMsg("100.000200", "unread")}}
	b := New(context.Background(), cfg, &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	if _, err := b.Wait(context.Background(), MinWaitTimeout); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	calls := api.calls()
	if len(calls) == 0 {
		t.Fatal("catch-up made no history call")
	}
	if calls[0].Oldest != "100.000100" || calls[0].Channel != testChannel {
		t.Errorf("history call = %+v, want oldest=100.000100 on the bound channel", calls[0])
	}
}

func TestWaitFailsWhenTheStreamClosesForGood(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	stream := newFakeStream()
	b := New(context.Background(), cfg, &fakeConnector{api: &fakeAPI{}, stream: stream})
	defer func() { _ = b.Close() }()

	close(stream.events)

	if _, err := b.Wait(context.Background(), MaxWaitTimeout); err == nil {
		t.Error("Wait() = nil error after the stream closed, want the disconnection reported")
	}
	if b.Status().Connected {
		t.Error("Status() still reports connected after the stream closed")
	}
}

func TestWaitRefusesASecondConcurrentBridge(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := New(ctx, cfg, &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()})
	defer func() { _ = first.Close() }()
	if _, err := first.Wait(ctx, 10*time.Millisecond); err != nil {
		t.Fatalf("first Wait() error = %v", err)
	}

	second := New(ctx, cfg, &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()})
	defer func() { _ = second.Close() }()

	_, err := second.Wait(ctx, 10*time.Millisecond)
	if !errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("second Wait() error = %v, want ErrAlreadyLocked", err)
	}

	// Once the first bridge lets go, the next session can take over.
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := second.Wait(ctx, 10*time.Millisecond); err != nil {
		t.Errorf("Wait() after the first bridge closed = %v, want it to succeed", err)
	}
}

// The upper bound exists because Claude Code aborts a stdio tool call that has
// produced no bytes for 30 minutes; 1500s keeps a five-minute margin.
func TestClampTimeout(t *testing.T) {
	tests := []struct {
		seconds int
		want    time.Duration
	}{
		{0, DefaultWaitTimeout},
		{-1, DefaultWaitTimeout},
		{1, MinWaitTimeout},
		{4, MinWaitTimeout},
		{5, 5 * time.Second},
		{300, 300 * time.Second},
		{1500, MaxWaitTimeout},
		{1501, MaxWaitTimeout},
		{86400, MaxWaitTimeout},
	}

	for _, tt := range tests {
		if got := ClampTimeout(tt.seconds); got != tt.want {
			t.Errorf("ClampTimeout(%d) = %v, want %v", tt.seconds, got, tt.want)
		}
	}

	if MaxWaitTimeout >= 30*time.Minute {
		t.Errorf("MaxWaitTimeout = %v, which reaches the client's 30-minute idle abort", MaxWaitTimeout)
	}
}

func TestWaitHonoursContextCancellation(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := New(ctx, cfg, &fakeConnector{api: &fakeAPI{}, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if _, err := b.Wait(ctx, MaxWaitTimeout); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait() error = %v, want context.Canceled", err)
	}
}
