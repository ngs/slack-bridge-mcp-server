package bridge

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

// threadedParent is a channel-surface message that has a thread hanging off
// it, described the way conversations.history describes one.
func threadedParent(ts, text, latestReply string, replies int) candidate {
	c := ownerMsg(ts, text)
	c.LatestReply = latestReply
	c.ReplyCount = replies
	return c
}

func reply(ts, threadTS, text string) candidate {
	c := ownerMsg(ts, text)
	c.ThreadTS = threadTS
	return c
}

// threadBridge returns a bridge whose cursor is at 100.000100, with the fake
// serving both the channel surface and the threads.
func threadBridge(ctx context.Context, t *testing.T, surface, replies []candidate) (*Bridge, *fakeAPI, *fakeStream) {
	t.Helper()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	api := &fakeAPI{history: surface, replies: replies}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })
	return b, api, stream
}

// The bug this fixes: the owner answers inside a thread while the laptop is
// asleep. conversations.history does not return thread replies at all, so
// before the thread pass existed the reply was simply never delivered — and
// with a thread-first convention, that is most of the conversation.
func TestCatchUpRecoversARepliedThreadWhileDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadBridge(ctx, t,
		[]candidate{
			ownerMsg("100.000100", "already answered"),
			threadedParent("100.000200", "here is the plan", "100.000400", 1),
		},
		[]candidate{
			threadedParent("100.000200", "here is the plan", "100.000400", 1),
			reply("100.000400", "100.000200", "sent from a thread while asleep"),
		},
	)

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	want := []string{"here is the plan", "sent from a thread while asleep"}
	if got := texts(result.Messages); !reflect.DeepEqual(got, want) {
		t.Fatalf("Wait() messages = %v, want %v", got, want)
	}

	// The reply has to arrive as a thread reply, or the agent answers into the
	// channel instead of the thread the owner is talking in.
	threadReply := result.Messages[1]
	if threadReply.ThreadTS != "100.000200" {
		t.Errorf("recovered reply thread_ts = %q, want 100.000200", threadReply.ThreadTS)
	}

	// A thread reply is often the newest thing in the channel, which is
	// exactly what the cursor should now be pointing at.
	if got := b.Status().LastTS; got != "100.000400" {
		t.Errorf("last_ts = %q, want it advanced past the recovered reply", got)
	}

	// And it is not fetched a second time.
	second, err := b.Wait(ctx, 20*testGrace)
	if err != nil {
		t.Fatalf("second Wait() error = %v", err)
	}
	if !second.TimedOut {
		t.Errorf("second Wait() = %+v, want a timeout; the reply was already delivered", second)
	}
	if got := api.snapshotReactions(); len(got) != 0 {
		t.Errorf("reactions = %+v with auto-ack off, want none", got)
	}
}

// The case that makes the scan ignore the cursor: the parent is ancient, the
// conversation in it is not. Bounding the parent scan by the cursor would find
// nothing here.
func TestCatchUpRecoversRepliesToAParentOlderThanTheCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadBridge(ctx, t,
		[]candidate{
			threadedParent("100.000050", "a thread from last week", "100.000300", 4),
			ownerMsg("100.000100", "already answered"),
		},
		[]candidate{
			reply("100.000300", "100.000050", "still talking in the old thread"),
		},
	)

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"still talking in the old thread"}) {
		t.Errorf("Wait() messages = %v, want the reply to the older parent", got)
	}

	// The scan that found the parent must not be bounded by the cursor, or the
	// parent is invisible and so is everything said in it.
	var unbounded bool
	for _, call := range api.calls() {
		if call.Oldest == "" {
			unbounded = true
		}
	}
	if !unbounded {
		t.Error("every history call was bounded by the cursor; threads with older parents can never be found")
	}
}

// The filters that apply everywhere else apply here too: a thread is a place
// other people talk, and only the owner drives the agent.
func TestCatchUpAppliesTheOwnerFilterToRecoveredReplies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	someoneElse := reply("100.000300", "100.000200", "a colleague chiming in")
	someoneElse.User = "U0COLLEAGUE"

	b, _, _ := threadBridge(ctx, t,
		[]candidate{
			ownerMsg("100.000100", "already answered"),
			threadedParent("100.000200", "here is the plan", "100.000400", 2),
		},
		[]candidate{
			someoneElse,
			reply("100.000400", "100.000200", "and the owner answering"),
		},
	)

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"here is the plan", "and the owner answering"}) {
		t.Errorf("Wait() messages = %v, want the colleague's reply left out", got)
	}
}

// Threads with nothing new in them must not be read at all; otherwise every
// reconnect walks the whole channel.
func TestCatchUpOnlyReadsThreadsRepliedToSinceTheCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadBridge(ctx, t,
		[]candidate{
			threadedParent("100.000050", "settled last week", "100.000060", 3),
			ownerMsg("100.000100", "already answered"),
			threadedParent("100.000200", "live discussion", "100.000400", 1),
		},
		[]candidate{
			reply("100.000400", "100.000200", "the new reply"),
		},
	)

	if _, err := b.Wait(ctx, MaxWaitTimeout); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	api.mu.Lock()
	calls := append([]RepliesRequest(nil), api.replyCalls...)
	api.mu.Unlock()

	if len(calls) != 1 || calls[0].ThreadTS != "100.000200" {
		t.Errorf("threads read = %+v, want only the one replied to since the cursor", calls)
	}
	if calls[0].Oldest != "100.000100" {
		t.Errorf("replies call oldest = %q, want the cursor so old replies are not refetched", calls[0].Oldest)
	}
}

// Seeding has to step past the threads too. The newest thing said in a channel
// is often a reply in an older thread, and a cursor set to the newest surface
// message would leave those replies looking new — handing the owner their own
// history back as if they had just sent it.
func TestFirstRunSeedsPastExistingThreadReplies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	api := &fakeAPI{
		history: []candidate{
			// The newest surface message is older than the conversation
			// happening in the thread above it.
			threadedParent("100.000200", "an old thread", "100.000900", 12),
			ownerMsg("100.000300", "the newest thing on the surface"),
		},
		replies: []candidate{reply("100.000900", "100.000200", "said long before the bridge existed")},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	defer func() { _ = b.Close() }()

	// The first wait seeds and returns nothing.
	if _, err := b.Wait(ctx, 20*testGrace); err != nil {
		t.Fatalf("first Wait() error = %v", err)
	}
	if got := b.Status().LastTS; got != "100.000900" {
		t.Errorf("seeded last_ts = %q, want 100.000900: the newest reply, not the newest surface message", got)
	}

	// Production queues a connected event on the way up, which sends the
	// bridge round for another catch-up. Nothing existing may come back.
	stream.events <- StreamEvent{Kind: StreamConnected}

	result, err := b.Wait(ctx, 20*testGrace)
	if err != nil {
		t.Fatalf("second Wait() error = %v", err)
	}
	if !result.TimedOut || len(result.Messages) != 0 {
		t.Errorf("Wait() = %+v, want nothing; every one of those messages predates the install", result)
	}
}

// When the cap bites, it has to spend its budget on the threads the owner is
// actually talking in — which is the ones replied to most recently, not the
// ones whose parent happens to be newest.
func TestCatchUpPrefersTheMostRecentlyRepliedThreads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var surface, replies []candidate
	// Newest parent first, as history returns them, with the liveliest thread
	// hanging off the oldest parent.
	for i := range maxThreadsPerCatchUp {
		parentTS := "100.000" + strconv.Itoa(900-i)
		replyTS := "100.000" + strconv.Itoa(400+i)
		surface = append(surface, threadedParent(parentTS, "quiet thread", replyTS, 1))
		replies = append(replies, reply(replyTS, parentTS, "an old aside"))
	}
	surface = append(surface, threadedParent("100.000150", "the live one", "100.000999", 30))
	replies = append(replies, reply("100.000999", "100.000150", "answered a minute ago"))

	b, api, _ := threadBridge(ctx, t, surface, replies)

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	var found bool
	for _, m := range result.Messages {
		if m.Text == "answered a minute ago" {
			found = true
		}
	}
	if !found {
		t.Error("the most recently replied thread was skipped for older ones; the cap must spend its budget by latest_reply")
	}

	api.mu.Lock()
	walked := len(api.replyCalls)
	api.mu.Unlock()
	if walked != maxThreadsPerCatchUp {
		t.Errorf("read %d threads, want the cap of %d", walked, maxThreadsPerCatchUp)
	}
}

// A fresh install joins the conversation. Reading every thread in the channel
// would be the replay that seeding the cursor exists to avoid.
func TestCatchUpReadsNoThreadsOnTheFirstEverRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	api := &fakeAPI{
		history: []candidate{threadedParent("100.000200", "an active thread", "100.000900", 12)},
		replies: []candidate{reply("100.000900", "100.000200", "chatter the agent was never part of")},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	result, err := b.Wait(ctx, 20*testGrace)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if !result.TimedOut || len(result.Messages) != 0 {
		t.Errorf("Wait() = %+v, want nothing replayed on a channel never read before", result)
	}

	api.mu.Lock()
	replyCalls := len(api.replyCalls)
	api.mu.Unlock()
	if replyCalls != 0 {
		t.Errorf("read %d threads on the first ever run, want 0", replyCalls)
	}
}

// The cap is what keeps a long absence from turning into hundreds of API
// calls. Hitting it is not an error, but it is a loss: the cursor advances
// past the threads that were not read, so their replies are missed for good
// rather than deferred. Catch-up says so on stderr; what must not happen is
// the whole thing failing.
func TestCatchUpStopsAfterTheThreadCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var surface, replies []candidate
	for i := range maxThreadsPerCatchUp + 5 {
		// Parents are numbered from 100.000200 upwards, each with a reply
		// newer than the cursor.
		parentTS := "100.000" + strconv.Itoa(200+i*2)
		replyTS := "100.000" + strconv.Itoa(201+i*2)
		surface = append(surface, threadedParent(parentTS, "parent", replyTS, 1))
		replies = append(replies, reply(replyTS, parentTS, "reply"))
	}

	b, api, _ := threadBridge(ctx, t, surface, replies)

	if _, err := b.Wait(ctx, MaxWaitTimeout); err != nil {
		t.Fatalf("Wait() error = %v, want the cap to truncate quietly rather than fail", err)
	}

	api.mu.Lock()
	walked := len(api.replyCalls)
	api.mu.Unlock()
	if walked != maxThreadsPerCatchUp {
		t.Errorf("read %d threads, want the cap of %d", walked, maxThreadsPerCatchUp)
	}
}

// A thread that no longer exists will not exist next time either, so catch-up
// steps over it. Wedging the relay on it forever would be far worse than
// losing that thread's replies.
func TestCatchUpSurvivesAnUnreadableThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadBridge(ctx, t,
		[]candidate{
			ownerMsg("100.000100", "already answered"),
			ownerMsg("100.000250", "a plain message"),
			threadedParent("100.000200", "a deleted thread", "100.000400", 1),
		},
		nil,
	)
	api.mu.Lock()
	api.repliesErr = fmt.Errorf("%w: thread_not_found", ErrThreadUnreadable)
	api.mu.Unlock()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v, want the unreadable thread skipped", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"a deleted thread", "a plain message"}) {
		t.Errorf("Wait() messages = %v, want the surface messages delivered anyway", got)
	}
}

// A thread Slack merely refused this time is a different matter: giving up on
// it quietly would move the cursor past replies nobody ever read. Catch-up
// fails instead, which is what keeps it retryable.
func TestCatchUpRetriesAfterATransientThreadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadBridge(ctx, t,
		[]candidate{
			ownerMsg("100.000100", "already answered"),
			threadedParent("100.000200", "here is the plan", "100.000400", 1),
		},
		[]candidate{
			reply("100.000400", "100.000200", "the reply that must not be skipped"),
		},
	)
	api.mu.Lock()
	api.repliesErr = errors.New("rate_limited")
	api.mu.Unlock()

	if _, err := b.Wait(ctx, MaxWaitTimeout); err == nil {
		t.Fatal("Wait() = nil error while a thread could not be read, want the failure surfaced")
	}
	if got := b.Status().LastTS; got != "100.000100" {
		t.Errorf("last_ts = %q after a failed catch-up, want it unchanged so the reply is fetched again", got)
	}

	api.mu.Lock()
	api.repliesErr = nil
	api.mu.Unlock()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"here is the plan", "the reply that must not be skipped"}) {
		t.Errorf("Wait() messages = %v, want the reply recovered on the retry", got)
	}
}

// A thread the owner worked through overnight can hold more replies than one
// page, and the cursor is about to move past all of them.
func TestCatchUpPagesThroughALongThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := threadBridge(ctx, t,
		[]candidate{
			ownerMsg("100.000100", "already answered"),
			threadedParent("100.000200", "here is the plan", "100.000400", 2),
		},
		[]candidate{
			reply("100.000300", "100.000200", "first page"),
			reply("100.000400", "100.000200", "second page"),
		},
	)
	// One reply per page, so the thread only comes back in full if the cursor
	// Slack hands over is followed.
	api.mu.Lock()
	api.repliesPageSize = 1
	api.mu.Unlock()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	want := []string{"here is the plan", "first page", "second page"}
	if got := texts(result.Messages); !reflect.DeepEqual(got, want) {
		t.Errorf("Wait() messages = %v, want %v; the thread was not paged to the end", got, want)
	}
}

// The cursor is about to move past everything this thread returns, so the
// thread has to be followed to its end rather than to the end of a page or two.
func TestCatchUpPagesAThreadToItsEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var replies []candidate
	for i := range 12 {
		replies = append(replies, reply("100.000"+strconv.Itoa(300+i), "100.000200", "reply "+strconv.Itoa(i)))
	}

	b, api, _ := threadBridge(ctx, t,
		[]candidate{
			ownerMsg("100.000100", "already answered"),
			threadedParent("100.000200", "here is the plan", "100.000311", 12),
		},
		replies,
	)
	// Two replies per page, so twelve replies take six pages — more than the
	// old budget of five.
	api.mu.Lock()
	api.repliesPageSize = 2
	api.mu.Unlock()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 13 {
		t.Fatalf("Wait() returned %d messages, want the parent and all 12 replies", len(result.Messages))
	}
	if got := b.Status().LastTS; got != "100.000311" {
		t.Errorf("last_ts = %q, want the newest reply; anything less means replies were skipped", got)
	}
}

// The live socket delivers thread replies as they happen, and catch-up finds
// the same reply on a reconnect. The owner must see the agent answer once.
func TestRecoveredReplyIsNotDuplicatedByTheLiveStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := threadBridge(ctx, t,
		[]candidate{
			ownerMsg("100.000100", "already answered"),
			threadedParent("100.000200", "here is the plan", "100.000400", 1),
		},
		[]candidate{
			reply("100.000400", "100.000200", "seen twice, delivered once"),
		},
	)

	// The socket delivered the reply, then reported the reconnect that sends
	// the bridge to history for the same window. Both copies are in play.
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000400", ThreadTS: "100.000200", User: testOwner, Text: "seen twice, delivered once"}}
	stream.events <- StreamEvent{Kind: StreamConnected}

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	want := []string{"here is the plan", "seen twice, delivered once"}
	if got := texts(result.Messages); !reflect.DeepEqual(got, want) {
		t.Errorf("Wait() messages = %v, want %v exactly once each", got, want)
	}
}
