package bridge

import (
	"context"
	"errors"
	"fmt"
	"log"
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
// how many of those it will actually read. Together they put a ceiling of
// twenty-one API calls on recovering a night's worth of threaded conversation.
const (
	threadScanLimit      = 200
	maxThreadsPerCatchUp = 20
)

// Bridge owns the Slack connection and the message cursor. All six MCP tools
// go through it.
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
	// pending holds messages read from the stream while merging, so nothing
	// is lost if a later step fails.
	pending []Message
	// indicator is the live "⏳ Working…" message, if one is running. At most
	// one exists at a time; see indicator.go.
	indicator *indicator
	// indicatorDone belongs to the most recent indicator, running or already
	// stopped. It outlives the indicator itself because the next one has to
	// wait for this one's chat.delete before posting its own message.
	indicatorDone <-chan struct{}
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
		PendingBacklogCount: len(b.pending),
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

// Close releases the lock and is safe to call on a bridge that never
// connected.
func (b *Bridge) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.connected = false
	b.stopIndicatorLocked()
	if b.lock == nil {
		return nil
	}
	lock := b.lock
	b.lock = nil
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
	}

	api, stream, err := b.connector.Connect(b.ctx, b.cfg)
	if err != nil {
		return err
	}

	b.api = api
	b.stream = stream
	b.connected = true
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
			// span the owner sees counted in the channel.
			b.startIndicator()
			b.autoAck(msgs)
			return WaitResult{Messages: msgs}, nil
		}

		select {
		case <-ctx.Done():
			return WaitResult{}, ctx.Err()

		case <-deadline.C:
			return WaitResult{Messages: []Message{}, TimedOut: true}, nil

		case evt, ok := <-stream.Events():
			if !ok {
				b.mu.Lock()
				b.connected = false
				b.mu.Unlock()
				return WaitResult{}, errors.New("the Slack connection closed")
			}
			if err := b.absorb(evt); err != nil {
				return WaitResult{}, err
			}
		}
	}
}

// absorb folds one stream event into the bridge's pending state.
//
// Both slack_wait and slack_ask pump the stream, so this is where an event is
// routed to the one that wants it: messages queue up for the next slack_wait
// whoever read them off the socket, and clicks go to the pending question.
func (b *Bridge) absorb(evt StreamEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch evt.Kind {
	case StreamMessage:
		b.pending = append(b.pending, evt.Message)
	case StreamInteraction:
		b.deliverInteraction(evt.Interaction)
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
	b.mu.Unlock()

	var fetched []Message
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
			fetched, err = catchUp(ctx, api, channel, b.cfg.Owner, lastTS)
			if err != nil {
				return nil, err
			}
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
	merged := mergeMessages(b.lastTS, fetched, b.pending)
	b.pending = nil
	if len(merged) == 0 {
		return nil, nil
	}

	newest := merged[len(merged)-1].TS
	if b.store != nil {
		if err := b.store.SetLastTS(channel, newest); err != nil {
			// Keep the messages rather than dropping them: a stale cursor
			// costs a duplicate after a restart, losing them costs the
			// owner a reply.
			log.Printf("could not persist the cursor: %v", err)
		}
	}
	b.lastTS = newest
	return merged, nil
}

// seedCursor records the newest message in the channel without returning it,
// establishing a starting point for a channel the bridge has never read.
func (b *Bridge) seedCursor(ctx context.Context, api API, channel string) (string, error) {
	page, err := api.History(ctx, HistoryRequest{Channel: channel, Limit: 1})
	if err != nil {
		return "", err
	}
	if len(page.Messages) == 0 {
		return "", nil
	}

	ts := page.Messages[0].TS
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
// Two caps keep the scan bounded, and both are real limits rather than
// theoretical ones. Only the newest threadScanLimit surface messages are
// examined, so a reply added to a thread that has since been pushed off that
// window is lost — accepted, because the alternative is walking the channel's
// whole history on every reconnect. And at most maxThreadsPerCatchUp threads
// are read, which is logged when it bites.
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

	var (
		messages []Message
		walked   int
	)
	for _, parent := range page.Messages {
		if parent.TS == "" || parent.LatestReply == "" || !tsLess(after, parent.LatestReply) {
			continue
		}
		if walked >= maxThreadsPerCatchUp {
			log.Printf("catch-up stopped after %d threads; the rest will be recovered as they are replied to", maxThreadsPerCatchUp)
			break
		}
		walked++

		replies, err := api.Replies(ctx, RepliesRequest{
			Channel:  channel,
			ThreadTS: parent.TS,
			Oldest:   after,
			Limit:    historyPageLimit,
		})
		if err != nil {
			// One unreadable thread — a deleted parent, most often — must not
			// wedge the relay: failing the whole catch-up would keep every
			// later message waiting behind it, every time.
			log.Printf("could not read a thread during catch-up: %s", logSafe(err.Error(), maxLoggedError))
			continue
		}

		for _, c := range replies.Messages {
			if msg, ok := accept(c, channel, owner); ok {
				messages = append(messages, msg)
			}
		}
	}

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
	d := time.Duration(seconds) * time.Second
	if d < MinWaitTimeout {
		return MinWaitTimeout
	}
	if d > MaxWaitTimeout {
		return MaxWaitTimeout
	}
	return d
}

// Post sends a message to the bound channel, connecting first if needed.
func (b *Bridge) Post(ctx context.Context, text, threadTS string) (string, error) {
	if text == "" {
		return "", errors.New("text is required")
	}

	// The reply is the answer the indicator was standing in for, so retire it
	// before the reply lands rather than after.
	b.stopIndicator()

	api, channel, err := b.apiForCall()
	if err != nil {
		return "", err
	}
	return api.Post(ctx, channel, threadTS, text)
}

// React adds an emoji reaction, the cheap way for the agent to signal "seen"
// without posting a message.
//
// It deliberately leaves the processing indicator alone: an ack means "seen,
// still working", which is exactly the situation the indicator is there for.
func (b *Bridge) React(ctx context.Context, ts, emoji string) error {
	if ts == "" {
		return errors.New("ts is required")
	}
	if emoji == "" {
		emoji = "eyes"
	}

	api, channel, err := b.apiForCall()
	if err != nil {
		return err
	}
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
func (b *Bridge) startIndicator() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.stopIndicatorLocked()
	if b.cfg.IndicatorDisabled || b.api == nil {
		return
	}

	grace, interval := b.cfg.indicatorTimings()
	b.indicator = newIndicator(b.ctx, b.api, b.cfg.Channel, grace, interval, b.indicatorDone)
	b.indicatorDone = b.indicator.done
	b.indicator.start()
}

// stopIndicator retires the running indicator, if any. It returns without
// waiting for the chat.delete, so no tool call is ever slowed down by it.
func (b *Bridge) stopIndicator() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopIndicatorLocked()
}

// stopIndicatorLocked is stopIndicator for callers that already hold b.mu. The
// done channel is kept behind, so the next indicator knows what it is waiting
// for.
func (b *Bridge) stopIndicatorLocked() {
	if b.indicator == nil {
		return
	}
	b.indicator.stop()
	b.indicatorDone = b.indicator.done
	b.indicator = nil
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
