package bridge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// Timeout bounds for slack_wait.
//
// MaxWaitTimeout is 1500s (25 minutes) because Claude Code aborts a stdio MCP
// tool call once no response bytes have arrived for 30 minutes. Returning
// well inside that window keeps a long poll from ever tripping the client's
// idle abort. DefaultWaitTimeout is a comfortable poll for a resident session.
const (
	MinWaitTimeout     = 5 * time.Second
	MaxWaitTimeout     = 1500 * time.Second
	DefaultWaitTimeout = 300 * time.Second
)

// historyPageLimit is the per-page size for catch-up, and maxHistoryPages caps
// how far back a single catch-up will walk. A machine asleep for a week should
// not replay a thousand messages into the agent's context.
const (
	historyPageLimit = 200
	maxHistoryPages  = 5
)

// Bounds on the thread pass of catch-up. threadScanLimit is how far back the
// bridge looks for threads that have been replied to; maxThreadsPerCatchUp is
// how many of those it will actually read.
//
// The worst case is one scan plus twenty threads of up to maxThreadCatchUpPages
// each, and it takes twenty separate threads each holding thousands of unread
// replies to get anywhere near it. A typical reconnect makes one call and finds
// nothing to read; a busy night makes a handful. The pass is bounded rather than budgeted deliberately: a budget
// spent halfway through a thread would leave the cursor advancing over replies
// that were never read, which is the failure this whole pass exists to stop.
const (
	threadScanLimit      = 200
	maxThreadsPerCatchUp = 20
	// maxThreadCatchUpPages is how far one thread is followed. Fifty pages is
	// ten thousand replies missed in a single thread while the bridge was
	// away, which is past any conversation and into a machine writing into the
	// channel.
	maxThreadCatchUpPages = 50
)

// shutdownIndicatorWait is how long Close waits for the indicator to clear
// itself from the channel. It covers the whole of what the indicator can still
// be doing when it is told to stop — a post or update already in flight,
// followed by the delete that finishes the job — plus a margin, so the wait
// ends because the work finished or failed rather than because the clocks
// disagreed and the process exited mid-cleanup.
const shutdownIndicatorWait = indicatorRequestTimeout + indicatorDeleteTimeout + 2*time.Second

// Bridge owns the Slack connection and the message cursors. Every MCP tool
// goes through it.
type Bridge struct {
	cfg       Config
	connector Connector

	// ctx bounds the Socket Mode goroutines; it is the server's context, so
	// the connection lives exactly as long as the MCP session.
	ctx context.Context

	mu        sync.Mutex
	api       API
	stream    Stream
	store     *Store
	lock      *Lock
	connected bool
	lastTS    string
	// needCatchUp is set on the first connect and on every reconnect. It is
	// the flag that makes sleep/wake safe: whatever the WebSocket missed is
	// still in Slack's history, and the next wait goes and gets it.
	needCatchUp bool
	// pending holds home-channel messages read from the stream while merging,
	// so nothing is lost if a later step fails.
	pending []Message
	// pendingThreads is the same for messages in conversation threads outside
	// the home channel. They are kept apart because the home channel's cursor
	// does not apply to them: every thread has its own, and one queue would let
	// a reply older than the home cursor be discarded as already seen.
	pendingThreads []Message
	// botUserID is this app's own user ID, which is what a mention looks like
	// in message text. It is learned when the connection opens.
	botUserID string
	// threads are the conversations open outside the home channel, and
	// threadCursors is how far each of them has been read. Both are restored
	// from the state file on connect, so a restart resumes a conversation
	// instead of waiting to be mentioned again.
	threads       map[threadKey]bool
	threadCursors map[threadKey]string
	// mentionCursor is how far through time the search for missed mentions has
	// looked.
	mentionCursor string
	// conversationsDegraded is set when Slack refuses the catch-up outside the
	// home channel for want of a scope. It lasts as long as the connection
	// because that is how long the answer can stay the same: scopes arrive with
	// a reinstall, and a reinstall is a new connection, which tries again.
	conversationsDegraded bool
	// indicator is the live "⏳ Working…" message, if one is running. At most
	// one exists at a time; see indicator.go.
	indicator *indicator
	// indicatorDone belongs to the most recent indicator, running or already
	// stopped. It outlives the indicator itself because the next one has to
	// wait for this one's chat.delete before posting its own message.
	indicatorDone <-chan struct{}
	// indicatorGeneration counts every start and stop, so a call that retired
	// an indicator can tell whether anything has happened since.
	indicatorGeneration uint64
	// ask is the slack_ask question waiting for a click, if any. At most one
	// is outstanding: a second question while one is pending is refused rather
	// than queued, so a click is never ambiguous.
	ask *pendingAsk
	// nameCache holds display names resolved for slack_history. It has its own
	// lock, so a users.info call never happens under b.mu.
	nameCache *nameCache
}

// New returns a Bridge that connects on first use. cfg may be incomplete; the
// error surfaces on the first tool call that needs Slack, while slack_status
// keeps working so the operator can see what is missing.
func New(ctx context.Context, cfg Config, connector Connector) *Bridge {
	if connector == nil {
		connector = SocketModeConnector{}
	}
	return &Bridge{ctx: ctx, cfg: cfg, connector: connector}
}

// Status is what slack_status reports.
type Status struct {
	Connected bool   `json:"connected"`
	Channel   string `json:"channel"`
	Owner     string `json:"owner"`
	LastTS    string `json:"last_ts"`
	// PendingBacklogCount is the number of messages already read from the
	// stream but not yet handed to a caller.
	PendingBacklogCount int `json:"pending_backlog_count"`
	// ConfigError names the missing environment variables, if any.
	ConfigError string `json:"config_error,omitempty"`
	// StateFile is where the cursor is persisted.
	StateFile string `json:"state_file,omitempty"`
}

// Status reports the bridge's state without connecting to Slack, so it stays
// useful precisely when something is misconfigured.
func (b *Bridge) Status() Status {
	b.mu.Lock()
	defer b.mu.Unlock()

	status := Status{
		Connected:           b.connected,
		Channel:             b.cfg.Channel,
		Owner:               b.cfg.Owner,
		LastTS:              b.lastTS,
		PendingBacklogCount: len(b.pending) + len(b.pendingThreads),
	}
	if err := b.cfg.Validate(); err != nil {
		status.ConfigError = err.Error()
	}
	if b.store != nil {
		status.StateFile = b.store.Path()
	} else if dir, err := b.cfg.ResolveStateDir(); err == nil {
		status.StateFile = NewStore(dir).Path()
	}
	return status
}

// Close retires the indicator and releases the lock. It is safe to call on a
// bridge that never connected.
//
// Unlike the tool calls, which never wait on the indicator, this one does: it
// runs as the process is about to exit, and the indicator's chat.delete lives
// on a goroutine that exiting would kill. Waiting for it here is the
// difference between the channel being left tidy and a "⏳ Working…" message
// sitting there until someone notices. The wait is bounded, so a Slack that
// never answers delays shutdown rather than preventing it.
func (b *Bridge) Close() error {
	b.mu.Lock()
	b.connected = false
	b.stopIndicatorLocked()
	done := b.indicatorDone
	lock := b.lock
	b.lock = nil
	b.mu.Unlock()

	if done != nil {
		timeout := time.NewTimer(shutdownIndicatorWait)
		defer timeout.Stop()
		select {
		case <-done:
		case <-timeout.C:
			log.Printf("gave up waiting for the processing indicator to clear itself from the channel")
		}
	}

	if lock == nil {
		return nil
	}
	return lock.Release()
}

// ensure performs the lazy connect: validate configuration, take the
// single-instance lock, load the persisted cursor, and open Slack. It is
// idempotent. The caller must hold b.mu.
func (b *Bridge) ensure() error {
	if b.connected {
		return nil
	}
	if err := b.cfg.Validate(); err != nil {
		return err
	}

	dir, err := b.cfg.ResolveStateDir()
	if err != nil {
		return err
	}

	if b.lock == nil {
		lock, err := AcquireLock(dir)
		if err != nil {
			return err
		}
		b.lock = lock
	}

	if b.store == nil {
		b.store = NewStore(dir)
		lastTS, err := b.store.LastTS(b.cfg.Channel)
		if err != nil {
			return err
		}
		b.lastTS = lastTS

		mentionCursor, err := b.store.MentionCursor()
		if err != nil {
			return err
		}
		b.mentionCursor = mentionCursor

		if err := b.loadThreadsLocked(); err != nil {
			return err
		}
	}

	api, stream, err := b.connector.Connect(b.ctx, b.cfg)
	if err != nil {
		return err
	}

	b.api = api
	// Its own user ID is how the bridge recognises a mention. Without it the
	// home channel still works and nothing else opens, which is why this is
	// read rather than required.
	b.botUserID = api.BotUserID()
	b.stream = stream
	b.connected = true
	// A fresh connection is the one moment the scopes can have changed, so the
	// catch-up outside the home channel is offered another go.
	b.conversationsDegraded = false
	// The first catch-up covers everything missed since the last session;
	// StreamConnected events later cover reconnects.
	b.needCatchUp = true
	return nil
}

// WaitResult is what slack_wait returns.
type WaitResult struct {
	Messages []Message `json:"messages"`
	TimedOut bool      `json:"timed_out"`
}

// Wait blocks until at least one owner message is available or the timeout
// expires.
//
// The first call connects and runs catch-up, so a backlog that accumulated
// while the session was down comes back immediately as an array rather than
// trickling in. After that it waits on the live stream, running catch-up again
// on every reconnect.
func (b *Bridge) Wait(ctx context.Context, timeout time.Duration) (WaitResult, error) {
	// Waiting again means the agent is done with whatever it was given last
	// time, even if it never posted a reply.
	b.stopIndicator()

	b.mu.Lock()
	if err := b.ensure(); err != nil {
		b.mu.Unlock()
		return WaitResult{}, err
	}
	stream := b.stream
	b.mu.Unlock()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		// Catch-up first: a pending backlog outranks waiting for something
		// new, and on a reconnect it is the only place missed messages are.
		msgs, err := b.drainCatchUp(ctx)
		if err != nil {
			return WaitResult{}, err
		}
		if len(msgs) > 0 {
			// From here the agent is working on these messages, which is the
			// span the owner sees counted. It is counted where they are
			// looking: the newest message is the one they just sent, so its
			// channel and thread are the conversation they are waiting on.
			b.startIndicator(newestConversation(msgs))
			b.autoAck(msgs)
			return WaitResult{Messages: msgs}, nil
		}

		// A pending question owns the click channel. Reading it here as well
		// would mean a click could be taken by this goroutine and handed
		// across to the question, and the question's own deadline cannot see
		// a click that is still in transit. A nil channel blocks forever,
		// which is exactly "leave those to the asker".
		clicks := stream.Interactions()
		if b.askPending() {
			clicks = nil
		}

		select {
		case <-ctx.Done():
			return WaitResult{}, ctx.Err()

		case <-deadline.C:
			return WaitResult{Messages: []Message{}, TimedOut: true}, nil

		case in, ok := <-clicks:
			if !ok {
				b.noteStreamClosed(stream)
				return WaitResult{}, errors.New("the Slack connection closed")
			}
			// A click arriving while nobody is asking anything is answered by
			// whoever is: routing it here keeps slack_wait from starving the
			// click channel while it holds the connection.
			b.routeInteraction(in)

		case evt, ok := <-stream.Events():
			if !ok {
				b.noteStreamClosed(stream)
				return WaitResult{}, errors.New("the Slack connection closed")
			}
			if err := b.absorb(evt); err != nil {
				return WaitResult{}, err
			}
		}
	}
}

// noteStreamClosed records that the live connection is gone, but only if the
// stream that closed is the one the bridge is still using.
//
// Two calls can be reading the same stream when it closes. The first reports
// the disconnection, and a tool call after it opens a replacement; if the
// second then cleared the flag unconditionally, the next call would open a
// third connection while the replacement was still consuming events, and the
// owner's messages would arrive on a socket nobody reads.
func (b *Bridge) noteStreamClosed(stream Stream) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stream == stream {
		b.connected = false
	}
}

// askPending reports whether a slack_ask question is waiting for a click.
func (b *Bridge) askPending() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ask != nil
}

// absorb folds one stream event into the bridge's pending state.
//
// Both slack_wait and slack_ask read the stream, so a message goes to the same
// place whichever of them happened to pick it up: the queue the next
// slack_wait drains.
func (b *Bridge) absorb(evt StreamEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch evt.Kind {
	case StreamMessage:
		// The socket relays every owner message in every channel the bot is in,
		// because which conversations are open changes while the session runs.
		// This is where that is decided.
		msg, ok := b.classifyLocked(evt.Message)
		if !ok {
			return nil
		}
		if msg.Channel == "" || msg.Channel == b.cfg.Channel {
			b.pending = append(b.pending, msg)
		} else {
			b.pendingThreads = append(b.pendingThreads, msg)
		}
	case StreamConnected, StreamDropped:
		// Both mean the live stream may have a hole in it. History is the
		// authority, so go re-read the window after the cursor.
		b.needCatchUp = true
	}
	return nil
}

// drainCatchUp runs catch-up when it is due, merges the result with anything
// already pending, and hands the messages over.
//
// The cursor is advanced and persisted only once the messages are about to be
// returned, so a failure anywhere earlier leaves the bridge ready to fetch
// them again on the next call rather than skipping past them.
func (b *Bridge) drainCatchUp(ctx context.Context) ([]Message, error) {
	b.mu.Lock()
	needCatchUp := b.needCatchUp
	api := b.api
	lastTS := b.lastTS
	channel := b.cfg.Channel
	owner := b.cfg.Owner
	b.mu.Unlock()

	var fetched, conversations []Message
	if needCatchUp {
		if lastTS == "" {
			// First run against this channel: seeding from the newest
			// message means a fresh install starts a conversation rather
			// than replaying the channel's entire history into the agent.
			seeded, err := b.seedCursor(ctx, api, channel)
			if err != nil {
				return nil, err
			}
			lastTS = seeded
		} else {
			var err error
			fetched, err = catchUp(ctx, api, channel, owner, lastTS)
			if err != nil {
				return nil, err
			}
		}

		// Everywhere else: the conversations opened by a mention, and any
		// mention that opened one while nobody was listening.
		var err error
		conversations, err = b.catchUpConversations(ctx, api, owner)
		if err != nil {
			return nil, err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if needCatchUp {
		b.needCatchUp = false
		if b.lastTS == "" {
			b.lastTS = lastTS
		}
	}

	// Live events and history overlap around a reconnect; merging deduplicates
	// by timestamp and drops anything at or before the cursor.
	home := mergeMessages(b.lastTS, fetched, b.pending)
	b.pending = nil

	threads := b.mergeThreadMessagesLocked(conversations, b.pendingThreads)
	b.pendingThreads = nil

	if len(home) == 0 && len(threads) == 0 {
		return nil, nil
	}

	if len(home) > 0 {
		newest := home[len(home)-1].TS
		if b.store != nil {
			if err := b.store.SetLastTS(channel, newest); err != nil {
				// Keep the messages rather than dropping them: a stale cursor
				// costs a duplicate after a restart, losing them costs the
				// owner a reply.
				log.Printf("could not persist the cursor: %v", err)
			}
		}
		b.lastTS = newest
	}
	for _, m := range threads {
		b.noteThreadDeliveredLocked(m)
	}

	return mergeConversations(home, threads), nil
}

// mergeThreadMessagesLocked combines what the threads outside the home channel
// have to offer, dropping anything already delivered and anything whose
// conversation is no longer open. The caller must hold b.mu.
//
// Each thread carries its own cursor, so this cannot lean on the home
// channel's: a reply in a conversation opened last week is older than the home
// cursor and still perfectly new.
func (b *Bridge) mergeThreadMessagesLocked(sources ...[]Message) []Message {
	merged := mergeConversations(sources...)

	kept := merged[:0]
	for _, m := range merged {
		key := threadKey{m.Channel, m.ThreadTS}
		if !b.threads[key] {
			// The conversation was closed while this message was in flight —
			// the thread was deleted, or the bot was removed from the channel.
			continue
		}
		if cursor := b.threadCursors[key]; cursor != "" && !tsLess(cursor, m.TS) {
			continue
		}
		kept = append(kept, m)
	}
	return kept
}

// noteThreadDeliveredLocked advances a conversation's cursor to a message just
// handed over. The caller must hold b.mu.
//
// The mention cursor is deliberately left alone here. It records how far the
// search for missed mentions has *looked*, not what has been delivered, and
// moving it forward on a live message would step over an older mention in
// another channel that the search has not reached yet. A mention found twice
// costs nothing: the second copy is behind its thread's cursor and is dropped.
func (b *Bridge) noteThreadDeliveredLocked(m Message) {
	if m.Channel == "" || m.ThreadTS == "" || m.TS == "" {
		return
	}

	key := threadKey{m.Channel, m.ThreadTS}
	if current := b.threadCursors[key]; current != "" && !tsLess(current, m.TS) {
		return
	}
	if b.threadCursors == nil {
		b.threadCursors = make(map[threadKey]string)
	}
	b.threadCursors[key] = m.TS

	if b.store == nil {
		return
	}
	if err := b.store.SetThread(m.Channel, m.ThreadTS, m.TS); err != nil {
		log.Printf("could not persist a conversation cursor: %v", err)
	}
}

// seedCursor records where the conversation already is, without returning any
// of it, establishing a starting point for a channel the bridge has never
// read.
//
// The newest surface message is not enough on its own. A thread hanging off an
// older message can hold the most recent thing anyone said, and the thread
// pass of catch-up would then find those replies sitting past the cursor and
// hand the owner's own history back to them as if it were new. So the seed is
// the newest timestamp anywhere in the scanned window — surface messages and
// their latest replies alike.
func (b *Bridge) seedCursor(ctx context.Context, api API, channel string) (string, error) {
	page, err := api.History(ctx, HistoryRequest{Channel: channel, Limit: threadScanLimit})
	if err != nil {
		return "", err
	}
	if len(page.Messages) == 0 {
		return "", nil
	}

	var ts string
	for _, m := range page.Messages {
		for _, candidateTS := range []string{m.TS, m.LatestReply} {
			if candidateTS != "" && (ts == "" || tsLess(ts, candidateTS)) {
				ts = candidateTS
			}
		}
	}
	if ts == "" {
		return "", nil
	}
	if b.store != nil && ts != "" {
		if err := b.store.SetLastTS(channel, ts); err != nil {
			log.Printf("could not persist the initial cursor: %v", err)
		}
	}
	return ts, nil
}

// catchUp returns the owner messages Slack has that the caller has not seen.
// conversations.history returns every author in the channel, so the same owner
// filter the live stream applies has to be applied here too.
//
// It takes two passes, because one is not enough: conversations.history only
// ever returns channel-surface messages, so a reply the owner typed inside a
// thread while the bridge was away is invisible to it. The second pass goes
// and finds those.
func catchUp(ctx context.Context, api API, channel, owner, after string) ([]Message, error) {
	if api == nil {
		return nil, errors.New("the bridge is not connected to Slack")
	}

	var (
		messages []Message
		cursor   string
	)
	for page := 0; page < maxHistoryPages; page++ {
		resp, err := api.History(ctx, HistoryRequest{
			Channel: channel,
			Oldest:  after,
			Cursor:  cursor,
			Limit:   historyPageLimit,
		})
		if err != nil {
			return nil, err
		}

		for _, c := range resp.Messages {
			if msg, ok := accept(c, channel, owner); ok {
				messages = append(messages, msg)
			}
		}

		cursor = resp.NextCursor
		if cursor == "" {
			break
		}
	}

	replies, err := catchUpThreads(ctx, api, channel, owner, after)
	if err != nil {
		return nil, err
	}

	// History pages arrive newest-first; mergeMessages sorts and deduplicates.
	return mergeMessages(after, messages, replies), nil
}

// catchUpThreads recovers thread replies newer than the cursor.
//
// A thread reply never appears in conversations.history, and its parent may be
// far older than the cursor — an hour of conversation can hang off a message
// from last week — so the window that finds the parents deliberately ignores
// the cursor. What identifies a thread worth reading is latest_reply, which
// Slack puts on every threaded parent: newer than the cursor means somebody
// has spoken in there since the bridge last looked.
//
// Two caps keep the scan bounded, and both lose messages when they bite, which
// is the trade accepted here rather than walking the channel's whole history
// on every reconnect. Only the newest threadScanLimit surface messages are
// examined, so a reply in a thread pushed off that window is never seen. And
// at most maxThreadsPerCatchUp threads are read: the cursor advances past the
// rest, so their replies are not recovered later either. Both are logged, and
// the threads read are the ones nearest the top of the channel, which is where
// a conversation the owner is actually having will be.
func catchUpThreads(ctx context.Context, api API, channel, owner, after string) ([]Message, error) {
	if after == "" {
		// A first run seeds the cursor from the newest message instead of
		// replaying, and reading every thread in the channel would be exactly
		// the replay that avoids.
		return nil, nil
	}

	page, err := api.History(ctx, HistoryRequest{Channel: channel, Limit: threadScanLimit})
	if err != nil {
		return nil, err
	}
	if page.HasMore {
		// The channel is busier than the scan window. Threads older than these
		// messages were not even looked at, so a reply in one of them is
		// missed without anything else noticing.
		log.Printf("catch-up scanned the newest %d messages for threads; older threads were not examined", threadScanLimit)
	}

	// Sorted by when each thread was last spoken in, not by how recently its
	// parent was posted, because that is what the cap has to choose between: a
	// week-old thread the owner answered ten minutes ago matters more than a
	// thread started this morning and quiet since.
	eligible := make([]candidate, 0, len(page.Messages))
	for _, parent := range page.Messages {
		if parent.TS == "" || parent.LatestReply == "" || !tsLess(after, parent.LatestReply) {
			continue
		}
		eligible = append(eligible, parent)
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		return tsLess(eligible[j].LatestReply, eligible[i].LatestReply)
	})

	var (
		messages []Message
		walked   int
		skipped  int
	)
	for _, parent := range eligible {
		if walked >= maxThreadsPerCatchUp {
			skipped++
			continue
		}
		walked++

		replies, err := readThread(ctx, api, channel, owner, parent.TS, after)
		if err != nil {
			if !errors.Is(err, ErrThreadUnreadable) {
				// Slack said "not now" rather than "not there". Failing the
				// whole catch-up is what keeps it retryable: the cursor stays
				// where it is, and the next attempt asks for the same window
				// again instead of stepping over replies it never read.
				return nil, err
			}
			// A thread that no longer exists will not exist next time either,
			// and failing forever on it would wedge every later message
			// behind it.
			log.Printf("skipping a thread that cannot be read: %s", logSafe(err.Error(), maxLoggedError))
			continue
		}
		messages = append(messages, replies...)
	}

	if skipped > 0 {
		// Said plainly, because it is a loss and not a deferral: the cursor is
		// about to move past these replies, and nothing goes back for them.
		log.Printf("catch-up read %d threads and skipped %d with newer replies; replies in the skipped threads will not be delivered",
			walked, skipped)
	}
	return messages, nil
}

// readThread collects the owner's replies in one thread, newer than after,
// following Slack's paging to the end of the thread.
//
// Paging to the end is the point: the caller is about to move the cursor past
// everything this returns, so a thread left half-read is a thread whose
// remaining replies are older than the new cursor and will never be asked for
// again. The page budget is set where a thread stops being a conversation
// someone had and starts being a data set — and reaching it is reported, since
// the same loss applies there.
func readThread(ctx context.Context, api API, channel, owner, threadTS, after string) ([]Message, error) {
	var (
		messages []Message
		cursor   string
	)
	for page := 0; page < maxThreadCatchUpPages; page++ {
		replies, err := api.Replies(ctx, RepliesRequest{
			Channel:  channel,
			ThreadTS: threadTS,
			Oldest:   after,
			Cursor:   cursor,
			Limit:    historyPageLimit,
		})
		if err != nil {
			return nil, err
		}

		for _, c := range replies.Messages {
			if msg, ok := accept(c, channel, owner); ok {
				messages = append(messages, msg)
			}
		}

		cursor = replies.NextCursor
		if cursor == "" {
			return messages, nil
		}
	}

	log.Printf("stopped reading a thread after %d pages of replies; anything past that is older than the new cursor and will not be delivered",
		maxThreadCatchUpPages)
	return messages, nil
}

// ClampTimeout turns the tool's timeout_seconds argument into a duration,
// substituting the default for zero and clamping to the supported range rather
// than rejecting out-of-range values, so a caller asking for an hour gets the
// longest safe poll instead of an error.
func ClampTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		return DefaultWaitTimeout
	}

	// Compare while the value is still a count of seconds. Converting first
	// multiplies by a billion, and a large enough number wraps time.Duration
	// round into a negative — so an hour would clamp correctly while
	// 1<<62 seconds would come out as the minimum, or as nothing at all.
	if seconds <= int(MinWaitTimeout/time.Second) {
		return MinWaitTimeout
	}
	if seconds >= int(MaxWaitTimeout/time.Second) {
		return MaxWaitTimeout
	}
	return time.Duration(seconds) * time.Second
}

// PostRequest is what slack_post sends, and where.
type PostRequest struct {
	Text string
	// ThreadTS replies inside a thread instead of on the channel surface.
	ThreadTS string
	// Channel is the conversation to speak in. Empty means the home channel,
	// which is what a session that never leaves it never has to think about.
	Channel string
}

// Post sends a message to a channel, connecting first if needed.
func (b *Bridge) Post(ctx context.Context, req PostRequest) (string, error) {
	if req.Text == "" {
		return "", errors.New("text is required")
	}

	// The reply is the answer the indicator was standing in for, so retire it
	// before the reply lands rather than after.
	retired := b.stopIndicator()

	api, _, err := b.apiForCall()
	if err != nil {
		b.restoreIndicator(ctx, retired)
		return "", err
	}

	ts, err := api.Post(ctx, b.channelFor(req.Channel), req.ThreadTS, req.Text)
	if err != nil {
		// The answer never landed, so the agent is still on the hook for it
		// and the channel should go back to saying so.
		b.restoreIndicator(ctx, retired)
		return "", err
	}
	return ts, nil
}

// restoreIndicator puts back the progress signal that a failed attempt to
// speak had retired, so the channel does not fall silent while the agent is
// still working.
//
// It restores only what was there. A reply that fails when no indicator was
// running means the agent was never handed anything to work on, and inventing
// a "⏳ Working…" for it would be a lie the owner cannot check. It also stays
// quiet when the call was cancelled: Slack may have accepted the message
// anyway, with the confirmation lost on the way back, and a cancelled call
// means nobody is waiting on this turn any more either.
func (b *Bridge) restoreIndicator(ctx context.Context, retired retiredIndicator) {
	if !retired.wasRunning || ctx.Err() != nil || b.ctx.Err() != nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Slack calls take time, and another call may have moved on while this one
	// was failing — a reply that succeeded, or a new turn with its own clock.
	// Undo only this call's own retirement, and only if it is still the last
	// thing that happened.
	if b.indicatorGeneration != retired.generation {
		return
	}
	// Back where it was, channel and thread included: the turn has not moved,
	// only the attempt to end it failed.
	b.startIndicatorLocked(retired.startedAt, retired.channel, retired.threadTS)
}

// ReactRequest is the message to mark and how.
type ReactRequest struct {
	TS    string
	Emoji string
	// Channel is where the message lives. Empty means the home channel: a ts
	// identifies a message only within its channel, so a reaction sent to the
	// wrong one lands on somebody else's message or on nothing at all.
	Channel string
}

// React adds an emoji reaction, the cheap way for the agent to signal "seen"
// without posting a message.
//
// It deliberately leaves the processing indicator alone: an ack means "seen,
// still working", which is exactly the situation the indicator is there for.
func (b *Bridge) React(ctx context.Context, req ReactRequest) error {
	if req.TS == "" {
		return errors.New("ts is required")
	}
	emoji := req.Emoji
	if emoji == "" {
		emoji = DefaultAutoAckEmoji
	}

	api, _, err := b.apiForCall()
	if err != nil {
		return err
	}
	channel, ts := b.channelFor(req.Channel), req.TS
	// The automatic receipt reaction may well have got there first with the
	// same emoji. The message is marked either way, which is all slack_ack
	// promises, so that is a success rather than something to report.
	if err := api.React(ctx, channel, ts, emoji); err != nil && !errors.Is(err, ErrAlreadyReacted) {
		return err
	}
	return nil
}

// startIndicator begins counting for the messages just handed to the agent,
// replacing any indicator still running so two of them can never coexist in the
// channel.
//
// The replacement is handed its predecessor's done channel and waits for it
// before posting anything, which is what makes "never two at once" hold even
// when the outgoing chat.delete is slower than the grace period. That wait
// happens on the new indicator's own goroutine, so this call still returns
// immediately.
//
// The indicator is given the bridge's own context rather than the tool call's:
// the call that starts it returns immediately, and a per-call context would be
// cancelled before the first tick.
//
// channel and threadTS say where the turn is happening; an empty channel means
// the home channel, and an empty thread means the channel surface.
func (b *Bridge) startIndicator(channel, threadTS string) {
	b.startIndicatorAt(time.Now(), channel, threadTS)
}

// startIndicatorAt is startIndicator with the clock set, so an indicator put
// back after a failed reply carries on counting from when the agent actually
// started rather than restarting at zero.
func (b *Bridge) startIndicatorAt(startedAt time.Time, channel, threadTS string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.startIndicatorLocked(startedAt, channel, threadTS)
}

func (b *Bridge) startIndicatorLocked(startedAt time.Time, channel, threadTS string) {
	b.stopIndicatorLocked()
	if b.cfg.IndicatorDisabled || b.api == nil {
		return
	}

	grace, interval := b.cfg.indicatorTimings()
	b.indicator = newIndicator(b.ctx, b.api, b.channelOr(channel), threadTS, grace, interval, b.indicatorDone)
	b.indicator.startedAt = startedAt
	b.indicatorDone = b.indicator.done
	b.indicatorGeneration++
	b.indicator.start()
}

// retiredIndicator is what stopping one leaves behind: enough to put it back
// if whatever retired it turned out not to happen.
type retiredIndicator struct {
	// wasRunning distinguishes "there was one and it is now stopped" from
	// "there was nothing to stop", which is what keeps a failed reply from
	// inventing a progress signal for work nobody handed the agent.
	wasRunning bool
	// startedAt is when the agent received the work, not when the indicator
	// was created, so a restored one shows the true elapsed time.
	startedAt time.Time
	// channel and threadTS are the surface the retired indicator was posting
	// to. A restore is the same turn resuming, so it has to go back to the same
	// place; re-deriving it from the messages is not possible here, since the
	// call that failed may not be the one that started the turn.
	channel  string
	threadTS string
	// generation is the bridge's indicator counter at the moment of this
	// retirement. It is what makes a restore safe to attempt after a slow
	// Slack call: if anything else has started or stopped an indicator since,
	// this retirement is no longer the current state and putting it back would
	// overwrite whatever replaced it.
	generation uint64
}

// stopIndicator retires the running indicator, if any. It returns without
// waiting for the chat.delete, so no tool call is ever slowed down by it.
func (b *Bridge) stopIndicator() retiredIndicator {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stopIndicatorLocked()
}

// stopIndicatorLocked is stopIndicator for callers that already hold b.mu. The
// done channel is kept behind, so the next indicator knows what it is waiting
// for.
func (b *Bridge) stopIndicatorLocked() retiredIndicator {
	// Every stop counts, including one that found nothing to stop. A call that
	// says "no indicator from here" is a decision, and a slow failing reply
	// must not be able to overrule it by putting its own back afterwards.
	b.indicatorGeneration++

	if b.indicator == nil {
		return retiredIndicator{generation: b.indicatorGeneration}
	}
	b.indicator.stop()
	b.indicatorDone = b.indicator.done
	startedAt := b.indicator.startedAt
	channel := b.indicator.channel
	threadTS := b.indicator.threadTS
	b.indicator = nil
	return retiredIndicator{
		wasRunning: true,
		startedAt:  startedAt,
		channel:    channel,
		threadTS:   threadTS,
		generation: b.indicatorGeneration,
	}
}

// newestConversation reports where the most recent of these messages was sent,
// which is the conversation the agent is now expected to answer in. An empty
// thread means the message was posted on a channel surface.
//
// The messages arrive oldest-first, so the last one is the newest. Only that
// one is consulted: a batch delivered after a reconnect can span several
// threads and several channels, and the owner is waiting where they spoke
// last, not where the backlog happens to start.
func newestConversation(msgs []Message) (channel, threadTS string) {
	if len(msgs) == 0 {
		return "", ""
	}
	newest := msgs[len(msgs)-1]
	return newest.Channel, newest.ThreadTS
}

// channelFor resolves a tool's optional channel argument, taking b.mu.
func (b *Bridge) channelFor(channel string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.channelOr(channel)
}

// channelOr is channelFor for callers that already hold b.mu.
//
// An empty channel means the home channel throughout the bridge. That is what
// keeps every tool's channel argument optional: a session that only ever talks
// in the home channel — which is every session before mentions existed — never
// has to name it.
func (b *Bridge) channelOr(channel string) string {
	if channel == "" {
		return b.cfg.Channel
	}
	return channel
}

// apiForCall connects if necessary and returns the Web API handle.
func (b *Bridge) apiForCall() (API, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.ensure(); err != nil {
		return nil, "", err
	}
	if b.api == nil {
		return nil, "", fmt.Errorf("the bridge is not connected to Slack")
	}
	return b.api, b.cfg.Channel, nil
}
