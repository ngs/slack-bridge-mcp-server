package bridge

import (
	"context"
	"errors"
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
// calls. Hitting it is not an error: what it does not recover now arrives with
// the next reply in that thread.
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

// One thread the bridge cannot read — a deleted parent, usually — must not
// stop the rest of catch-up. Wedging the relay is far worse than losing that
// thread's replies.
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
	api.repliesErr = errors.New("thread_not_found")
	api.mu.Unlock()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v, want the unreadable thread skipped", err)
	}
	if got := texts(result.Messages); !reflect.DeepEqual(got, []string{"a deleted thread", "a plain message"}) {
		t.Errorf("Wait() messages = %v, want the surface messages delivered anyway", got)
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
