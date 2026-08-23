package bridge

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// historyBridge returns a bridge over a channel where several people, a bot
// and an incoming webhook have all been talking.
func historyBridge(ctx context.Context, t *testing.T) (*Bridge, *fakeAPI) {
	t.Helper()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{
		history: []candidate{
			ownerMsg("100.000200", "what do we do about the migration?"),
			{Channel: testChannel, User: "U0COLLEAGUE", Text: "roll it back", TS: "100.000300", ReplyCount: 2},
			{Channel: testChannel, BotID: "B0DEPLOY", SubType: "bot_message", Username: "deploybot", Text: "deploy failed", TS: "100.000400"},
			{Channel: testChannel, BotID: "B0HOOK", Username: "PagerDuty", Text: "incident opened", TS: "100.000500"},
		},
		names: map[string]string{
			testOwner:     "Owner",
			"U0COLLEAGUE": "Colleague",
		},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })
	return b, api
}

// The reason the tool exists: the owner asks the agent to read a discussion it
// was never part of, so every author has to come back, not just the owner.
func TestHistoryReturnsEveryAuthor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _ := historyBridge(ctx, t)

	result, err := b.History(ctx, ReadRequest{})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(result.Messages) != 4 {
		t.Fatalf("History() returned %d messages, want all 4 authors", len(result.Messages))
	}

	// Oldest first, so the model reads the discussion in the order it happened.
	wantTS := []string{"100.000200", "100.000300", "100.000400", "100.000500"}
	var gotTS []string
	for _, m := range result.Messages {
		gotTS = append(gotTS, m.TS)
	}
	if !reflect.DeepEqual(gotTS, wantTS) {
		t.Errorf("History() timestamps = %v, want %v oldest first", gotTS, wantTS)
	}

	names := []string{"Owner", "Colleague", "deploybot", "PagerDuty"}
	for i, want := range names {
		if got := result.Messages[i].UserName; got != want {
			t.Errorf("message %d user_name = %q, want %q", i, got, want)
		}
	}

	if result.Messages[0].Bot || result.Messages[1].Bot {
		t.Error("a human message is marked as a bot post")
	}
	if !result.Messages[2].Bot || !result.Messages[3].Bot {
		t.Error("a bot or webhook post is not marked as one")
	}
	// The thread is the part the model cannot see from here, so it has to know
	// there is one.
	if result.Messages[1].ReplyCount != 2 {
		t.Errorf("reply_count = %d, want 2 so the caller knows to fetch the thread", result.Messages[1].ReplyCount)
	}
}

// Reading the channel must be completely inert: whatever slack_wait was going
// to deliver, it still delivers.
func TestHistoryLeavesTheWaitPipelineAlone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)

	// Connect and drain the catch-up first, so what the cursor does next is
	// down to slack_history alone.
	if _, err := b.Wait(ctx, MaxWaitTimeout); err != nil {
		t.Fatalf("initial Wait() error = %v", err)
	}

	// A message arrives and is read off the stream, but not yet delivered.
	b.mu.Lock()
	b.pending = append(b.pending, Message{TS: "100.000600", User: testOwner, Text: "still owed to the agent"})
	b.mu.Unlock()
	before := b.Status()

	if _, err := b.History(ctx, ReadRequest{}); err != nil {
		t.Fatalf("History() error = %v", err)
	}

	after := b.Status()
	if after.LastTS != before.LastTS {
		t.Errorf("last_ts moved from %q to %q; slack_history must not consume messages", before.LastTS, after.LastTS)
	}
	if after.PendingBacklogCount != before.PendingBacklogCount {
		t.Errorf("pending backlog = %d, want it untouched at %d", after.PendingBacklogCount, before.PendingBacklogCount)
	}
	if got := api.snapshotReactions(); len(got) != 0 {
		t.Errorf("slack_history reacted to %d messages, want none", len(got))
	}
	if got := api.snapshotPosts(); len(got) != 0 {
		t.Errorf("slack_history posted %d messages, want none", len(got))
	}

	// And the message it did not consume is still there for the next wait.
	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 1 || result.Messages[0].Text != "still owed to the agent" {
		t.Errorf("Wait() messages = %+v, want the pending message intact", result.Messages)
	}
}

func TestHistoryClampsTheLimitAndPassesTheWindowThrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)

	tests := []struct {
		asked int
		want  int
	}{
		{0, DefaultHistoryLimit},
		{-5, DefaultHistoryLimit},
		{1, 1},
		{25, 25},
		{1000, MaxHistoryLimit},
	}
	for _, tt := range tests {
		if got := clampHistoryLimit(tt.asked); got != tt.want {
			t.Errorf("clampHistoryLimit(%d) = %d, want %d", tt.asked, got, tt.want)
		}
	}

	if _, err := b.History(ctx, ReadRequest{Limit: 2, Oldest: "100.000100", Latest: "100.000900"}); err != nil {
		t.Fatalf("History() error = %v", err)
	}

	calls := api.calls()
	last := calls[len(calls)-1]
	if last.Limit != 2 || last.Oldest != "100.000100" || last.Latest != "100.000900" {
		t.Errorf("history call = %+v, want the window and limit passed through", last)
	}
}

// Both bounds are exclusive, because Slack's are: `oldest` is the wait
// cursor, and a message the agent already answered must not come back through
// a different door.
func TestHistoryWindowBoundsAreExclusive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _ := historyBridge(ctx, t)

	result, err := b.History(ctx, ReadRequest{Oldest: "100.000200", Latest: "100.000500"})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}

	var got []string
	for _, m := range result.Messages {
		got = append(got, m.TS)
	}
	want := []string{"100.000300", "100.000400"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("History() timestamps = %v, want %v: the messages on the bounds are outside the window", got, want)
	}
}

// Slack cutting the window short is worth reporting: the model should know it
// is looking at part of a conversation.
func TestHistoryReportsATruncatedWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _ := historyBridge(ctx, t)

	result, err := b.History(ctx, ReadRequest{Limit: 2})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("History() returned %d messages, want 2", len(result.Messages))
	}
	if !result.HasMore {
		t.Error("has_more = false on a truncated window, want true")
	}
}

func TestHistoryReadsAThreadWhenGivenOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)
	api.mu.Lock()
	api.replies = []candidate{
		{Channel: testChannel, User: "U0COLLEAGUE", Text: "roll it back", TS: "100.000300", ThreadTS: "100.000300"},
		{Channel: testChannel, User: testOwner, Text: "agreed", TS: "100.000350", ThreadTS: "100.000300"},
	}
	api.mu.Unlock()

	result, err := b.History(ctx, ReadRequest{ThreadTS: "100.000300"})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("History() returned %d messages, want the thread's 2", len(result.Messages))
	}
	if result.Messages[1].Text != "agreed" || result.Messages[1].ThreadTS != "100.000300" {
		t.Errorf("thread reply = %+v, want the reply with its thread_ts", result.Messages[1])
	}

	api.mu.Lock()
	replyCalls, historyCalls := len(api.replyCalls), len(api.historyCalls)
	req := api.replyCalls[0]
	api.mu.Unlock()

	if replyCalls != 1 || historyCalls != 0 {
		t.Errorf("made %d replies and %d history calls, want the thread read instead of the channel", replyCalls, historyCalls)
	}
	if req.ThreadTS != "100.000300" || req.Channel != testChannel {
		t.Errorf("replies call = %+v, want the thread on the bound channel", req)
	}
}

// "Read this thread" means how the discussion ended, not how it started.
// conversations.replies walks forward from the parent, so passing the limit
// straight through would answer with the oldest few — for a limit of one, the
// parent alone.
func TestHistoryKeepsTheNewestEndOfALimitedThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)
	api.mu.Lock()
	api.replies = []candidate{
		{Channel: testChannel, User: testOwner, Text: "the question", TS: "100.000300", ThreadTS: "100.000300"},
		{Channel: testChannel, User: "U0COLLEAGUE", Text: "an early guess", TS: "100.000310", ThreadTS: "100.000300"},
		{Channel: testChannel, User: "U0COLLEAGUE", Text: "what we actually decided", TS: "100.000320", ThreadTS: "100.000300"},
	}
	api.mu.Unlock()

	result, err := b.History(ctx, ReadRequest{ThreadTS: "100.000300", Limit: 2})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}

	var got []string
	for _, m := range result.Messages {
		got = append(got, m.Text)
	}
	want := []string{"an early guess", "what we actually decided"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("History() messages = %v, want %v: the newest end, still oldest first", got, want)
	}
	if !result.HasMore {
		t.Error("has_more = false after dropping the start of the thread, want true")
	}

	// And the one-message case, which is where the old behaviour was at its
	// most useless: it returned the parent and nothing else.
	single, err := b.History(ctx, ReadRequest{ThreadTS: "100.000300", Limit: 1})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(single.Messages) != 1 || single.Messages[0].Text != "what we actually decided" {
		t.Errorf("History() with limit 1 = %+v, want the last thing said", single.Messages)
	}
}

// A thread longer than one page has to be walked to its end before the newest
// messages can be picked out of it.
func TestHistoryPagesAThreadBeforeTakingItsNewestMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)
	api.mu.Lock()
	api.replies = []candidate{
		{Channel: testChannel, User: testOwner, Text: "first", TS: "100.000300", ThreadTS: "100.000300"},
		{Channel: testChannel, User: testOwner, Text: "second", TS: "100.000310", ThreadTS: "100.000300"},
		{Channel: testChannel, User: testOwner, Text: "third", TS: "100.000320", ThreadTS: "100.000300"},
	}
	api.repliesPageSize = 1
	api.mu.Unlock()

	result, err := b.History(ctx, ReadRequest{ThreadTS: "100.000300", Limit: 2})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}

	var got []string
	for _, m := range result.Messages {
		got = append(got, m.Text)
	}
	if !reflect.DeepEqual(got, []string{"second", "third"}) {
		t.Errorf("History() messages = %v, want the last two of a paged thread", got)
	}
}

// The users:read scope is optional in practice: an app installed before it was
// added has not got it, and the tool has to stay useful on raw IDs.
func TestHistoryFallsBackToUserIDsWhenNamesCannotBeResolved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)
	api.mu.Lock()
	api.nameErr = errors.New("missing_scope")
	api.mu.Unlock()

	result, err := b.History(ctx, ReadRequest{})
	if err != nil {
		t.Fatalf("History() error = %v, want the messages returned with raw IDs", err)
	}
	if got := result.Messages[0].UserName; got != testOwner {
		t.Errorf("user_name = %q with users.info failing, want the raw ID %q", got, testOwner)
	}

	// Two distinct users, and neither is asked about twice within the call:
	// the lookup already failed, and it will fail the same way again.
	api.mu.Lock()
	lookups := api.nameLookups
	api.mu.Unlock()
	if lookups != 2 {
		t.Errorf("users.info called %d times for 2 distinct failing users, want 2", lookups)
	}
}

// Display names change rarely and channels repeat authors constantly, so the
// same ID must not cost a lookup every time it appears.
func TestHistoryResolvesEachNameOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)
	api.mu.Lock()
	api.history = append(api.history, ownerMsg("100.000600", "and another thing"))
	api.mu.Unlock()

	for range 2 {
		if _, err := b.History(ctx, ReadRequest{}); err != nil {
			t.Fatalf("History() error = %v", err)
		}
	}

	api.mu.Lock()
	lookups := api.nameLookups
	api.mu.Unlock()

	// Two distinct users across five messages and two calls.
	if lookups != 2 {
		t.Errorf("users.info called %d times, want 2 — one per distinct user, cached after that", lookups)
	}
}

// A webhook post carries the name it wants to be shown under and no user at
// all, so there is nothing to look up.
func TestHistoryDoesNotLookUpNamesForWebhookPosts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)
	api.mu.Lock()
	api.history = []candidate{
		{Channel: testChannel, BotID: "B0HOOK", Username: "PagerDuty", Text: "incident opened", TS: "100.000500"},
	}
	api.mu.Unlock()

	result, err := b.History(ctx, ReadRequest{})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if result.Messages[0].UserName != "PagerDuty" || result.Messages[0].User != "" {
		t.Errorf("webhook post = %+v, want it named by its own username with no user ID", result.Messages[0])
	}

	api.mu.Lock()
	lookups := api.nameLookups
	api.mu.Unlock()
	if lookups != 0 {
		t.Errorf("users.info called %d times for a post with no user, want 0", lookups)
	}
}
