package bridge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
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

	// catchUpSlot serialises catch-up, which is the one place a call goes to
	// Slack and back before committing. A channel rather than a mutex so that
	// a caller waiting for its turn can give up when its own context does.
	catchUpSlot chan struct{}

	mu        sync.Mutex
	api       API
	stream    Stream
	store     *Store
	lock      *Lock
	connected bool
	lastTS    string
	// catchUpEpoch counts the requests. A catch-up clears needCatchUp only if
	// the epoch has not moved since it started, so a request made while it was
	// in flight survives it.
	catchUpEpoch uint64
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
	// pendingReactions holds the emoji events read from the stream but not yet
	// handed over. They are a queue of their own because they are not
	// messages: no cursor applies to them, they are never merged with history,
	// and a reaction older than the home cursor is still news.
	pendingReactions []Reaction
	// heldReactions are the ones that matched no conversation while a catch-up
	// was outstanding, waiting for the window that may explain them.
	heldReactions []heldReaction
	// catchUpRuns counts the catch-ups that have finished. It is how a held
	// reaction knows its wait is over.
	catchUpRuns uint64
	// seenReactions and seenReactionOrder are the window of reactions already
	// queued, against Slack redelivering an envelope it was not acknowledged
	// for. They live here rather than on the stream because a reconnect
	// replaces the stream, and the redelivery can arrive on the replacement.
	seenReactions     map[string]struct{}
	seenReactionOrder []string
	// reactionsDropped records a loss the agent has not been told about yet. A
	// stream carries its own marker only as long as it lives, so a loss on a
	// connection that then died would otherwise go unreported.
	reactionsDropped bool
	// stopConnection ends everything the current connection started: the pump
	// that owns its stream, and the goroutines the connector runs behind it. It
	// is replaced with each connection and called before the next one starts.
	stopConnection context.CancelFunc
	// connCtx is what stopConnection ends. A catch-up follows it as well as its
	// own caller, so a fetch left behind by a connection that has been replaced
	// gives up instead of holding the catch-up slot until the tool call that
	// started it times out.
	connCtx context.Context
	// botUserID is this app's own user ID, which is what a mention looks like
	// in message text. It is learned when the connection opens.
	botUserID string
	// threads are the conversations open outside the home channel, and
	// threadCursors is how far each of them has been read. Both are restored
	// from the state file on connect, so a restart resumes a conversation
	// instead of waiting to be mentioned again.
	threads map[threadKey]bool
	// closed marks the bridge shut down, so a call still running cannot open a
	// connection behind Close.
	closed bool
	// pendingFull and threadsFull keep each queue's overflow notice to one line
	// per episode.
	pendingFull bool
	threadsFull bool
	// deliveredMessages remembers what has been handed over, so a message the
	// socket delivers twice is queued once.
	//
	// Slack retries an envelope it has not been acknowledged for, and the
	// acknowledgement goes out before the message is put on the stream: a
	// receipt lost on the way back brings the message round again, on this
	// connection or the next. The cursor cannot answer for it — a live message
	// is deliberately not filtered by the cursor, because on the run that
	// seeds it the owner's message can be older than the seed — so what has
	// actually been delivered is remembered instead.
	deliveredMessages map[string]struct{}
	deliveredOrder    []string
	// holeDiscards counts the catch-ups thrown away in a row because a hole
	// opened while they were reading. One is ordinary; a run of them is a
	// socket flapping, and what it costs is the messages already queued.
	holeDiscards int
	// threadsSkipped marks conversations left unread for want of budget. It is
	// its own flag rather than another meaning for needCatchUp: what it asks
	// for is one more walk through the threads, not a re-read of the home
	// channel and another scan for mentions — and left on needCatchUp it never
	// cleared, so a session with more open conversations than one pass can read
	// ran a full catch-up on every wake and held every out-of-scope reaction
	// for ever.
	threadsSkipped bool
	// skippedThreads are the conversations the last walk could not reach, so
	// the next one starts with them rather than with whatever the map yields
	// first.
	skippedThreads map[threadKey]struct{}
	// connectAnnounced marks this connection's own hello as seen. The socket
	// announces itself once per connection, after Connect has returned, and
	// the catch-up that covers what was missed while the session was down is
	// asked for when the connection is opened rather than when it says hello.
	connectAnnounced bool
	// overflowNoted marks an overflow the stream is holding that has already
	// been answered with a request to read the window again. The stream keeps
	// reporting it until it has room to announce it, and this is what keeps
	// the pump from raising a fresh hole on every look.
	overflowNoted atomic.Bool
	// holeEpoch counts the holes the live stream has been found to have: a
	// reconnect, an overflow, a message the socket refused. It is stamped on a
	// catch-up in flight the same way catchUpEpoch is, but it means something
	// stronger — what that catch-up read cannot be trusted at all, because
	// nobody knows where the hole is.
	holeEpoch uint64
	// seedUnwritten marks a seed established in memory by a call that could
	// not write it — one whose connection was replaced underneath it. The next
	// call to commit on the live connection writes it, whether or not it has
	// anything of its own to record.
	seedUnwritten bool
	// cursorSeeded records that the home cursor has been established, which an
	// empty channel does with an empty timestamp.
	cursorSeeded bool
	// preSeedRefused records that the queue overflowed before the cursor
	// existed, so the seed read over it must not become the cursor.
	preSeedRefused bool
	// pumpDone closes when the current connection's pump has stopped. Close
	// waits on it before the state writer is stopped, so a cursor the pump was
	// in the middle of recording still reaches the file.
	pumpDone chan struct{}
	// stateDirty holds the cursor changes that have not reached the state file,
	// keyed so that a later change to the same thing replaces an earlier one.
	// Nothing writes that file from under b.mu: it is the lock the pump holds
	// while it applies what the socket delivered, and a slow disk beneath it
	// stops the only reader the connection has. stateWake tells the writer
	// there is something to take, stopStateWrites tells it to flush and stop,
	// and stateWritesDone closes once it has.
	stateDirty map[stateKey]stateWrite
	// stateWriteFailing keeps a failing state file to one line per episode.
	stateWriteFailing bool
	// stateWriteMu guards the fence and the write it fences, so that deciding
	// a write may start and marking it as started are one step. Held for those
	// two flags only, never across a write, and never together with b.mu.
	stateWriteMu sync.Mutex
	// stateWriting marks a write that is inside the store right now, so
	// shutdown can wait out the one the fence was too late for.
	stateWriting bool
	// stateWriteAttempts counts the writes handed to the store, retries
	// included. It exists so that a refused write can be seen to have been
	// tried again rather than merely still queued.
	stateWriteAttempts atomic.Uint64
	// stateFenced stops the writer for good, whatever it is in the middle of.
	// It is set when shutdown has waited as long as it can and is about to
	// release the lock that keeps another session off this file. Guarded by
	// stateWriteMu together with stateWriting: a write that has passed the
	// fence has to be visible to whoever set it.
	stateFenced bool
	// stateClosed marks the writer as flushed and stopped, so a call still
	// running at shutdown does not start another one behind it.
	stateClosed     bool
	stateWake       chan struct{}
	stopStateWrites chan struct{}
	stateWritesDone chan struct{}
	threadCursors   map[threadKey]string
	// mentionCursor is how far through time the search for missed mentions has
	// looked.
	mentionCursor string
	// connGeneration counts the connections this bridge has opened, starting at
	// one. A catch-up runs without b.mu held and can still be in flight when the
	// connection under it is replaced, so what it reports is tagged with the
	// generation it belongs to and ignored if that generation is over.
	connGeneration uint64
	// degradedGeneration is the connection whose catch-up outside the home
	// channel was refused for want of a scope; zero is none. It is a generation
	// rather than a flag because the answer only lasts as long as the
	// connection: scopes arrive with a reinstall, and a reinstall is a new
	// connection, which tries again.
	degradedGeneration uint64
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
	// pendingSubs are the calls currently blocked waiting for the queue above
	// to grow. Both slack_wait and slack_ask read the same stream, so the call
	// that takes a message off it is not necessarily the one that wants it;
	// this is how the one that does finds out.
	//
	// It is a set of channels rather than one shared channel because a wakeup
	// on a shared channel goes to whichever reader happens to take it, and the
	// other one carries on blocked. Every subscriber has to hear it.
	pendingSubs map[chan struct{}]struct{}
	// activeWaits and activeAsks count the calls currently listening for the
	// owner. They are what the presence file publishes, and what a Stop hook
	// outside this process reads to decide whether the session left anybody
	// attending before it ended. See presence.go.
	activeWaits int
	activeAsks  int
	// presenceWarned keeps a failing presence file to one log line.
	presenceWarned bool
}

// subscribePending registers for notice that the pending queue has grown, and
// returns the channel that notice arrives on.
//
// The channel is buffered so a notification sent between the caller's drain and
// its select is kept rather than dropped, which is the race the subscription
// exists to close in the first place.
func (b *Bridge) subscribePending() chan struct{} {
	ch := make(chan struct{}, 1)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pendingSubs == nil {
		b.pendingSubs = make(map[chan struct{}]struct{})
	}
	b.pendingSubs[ch] = struct{}{}
	return ch
}

func (b *Bridge) unsubscribePending(ch chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pendingSubs, ch)
}

// notifyPendingLocked tells every waiting call that there may be something for
// it now. The caller must hold b.mu.
//
// The send is non-blocking against a buffered channel, so a subscriber that has
// not consumed its last notification simply keeps it: one wakeup and two are
// the same instruction, which is to go and drain.
func (b *Bridge) notifyPendingLocked() {
	for ch := range b.pendingSubs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// New returns a Bridge that connects on first use. cfg may be incomplete; the
// error surfaces on the first tool call that needs Slack, while slack_status
// keeps working so the operator can see what is missing.
func New(ctx context.Context, cfg Config, connector Connector) *Bridge {
	if connector == nil {
		connector = SocketModeConnector{}
	}
	return &Bridge{
		ctx:         ctx,
		cfg:         cfg,
		connector:   connector,
		catchUpSlot: make(chan struct{}, 1),
	}
}

// requestCatchUpLocked asks for the window to be re-read, and stamps the
// request so a catch-up already in flight cannot answer it. The caller must
// hold b.mu.
//
// The stamp is what stops a reconnect, an overflow or a refused message from
// being swallowed: a catch-up that started before the request went to Slack
// with the old window in mind, and clearing the flag on its way back would
// leave nothing to fetch what it never asked for.
func (b *Bridge) requestCatchUpLocked() {
	b.needCatchUp = true
	b.catchUpEpoch++
	b.notifyPendingLocked()
}

// noteDeliveredLocked remembers a batch on its way to the agent. The caller
// must hold b.mu.
func (b *Bridge) noteDeliveredLocked(batches ...[]Message) {
	for _, batch := range batches {
		for _, m := range batch {
			if m.TS == "" {
				continue
			}
			key := deliveredKey(m)
			if _, ok := b.deliveredMessages[key]; ok {
				continue
			}
			if b.deliveredMessages == nil {
				b.deliveredMessages = make(map[string]struct{}, messageDedupWindow)
			}
			b.deliveredMessages[key] = struct{}{}
			b.deliveredOrder = append(b.deliveredOrder, key)
			if len(b.deliveredOrder) > messageDedupWindow {
				delete(b.deliveredMessages, b.deliveredOrder[0])
				b.deliveredOrder = b.deliveredOrder[1:]
			}
		}
	}
}

// undeliveredLocked drops the messages that have been handed over already. The
// caller must hold b.mu.
func (b *Bridge) undeliveredLocked(msgs []Message) []Message {
	if len(b.deliveredMessages) == 0 || len(msgs) == 0 {
		return msgs
	}
	kept := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		if b.alreadyDeliveredLocked(m) {
			continue
		}
		kept = append(kept, m)
	}
	return kept
}

// alreadyDeliveredLocked reports whether this message has been handed over
// already. The caller must hold b.mu.
func (b *Bridge) alreadyDeliveredLocked(m Message) bool {
	if m.TS == "" {
		return false
	}
	_, ok := b.deliveredMessages[deliveredKey(m)]
	return ok
}

// requestCatchUpForHoleLocked asks for the window to be re-read because the
// live stream has a hole in it: a reconnect, an overflow, a stream that has
// refused a message and could not say so. The caller must hold b.mu.
//
// A hole is different in kind from a refusal, and the difference decides what a
// catch-up in flight may do with what it read. Nobody knows where a hole is, so
// a catch-up that was reading while one opened cannot tell whether what it has
// belongs before or after it: it throws the lot away and goes again. A message
// this bridge refused for want of room is the other way round — the queue was
// full, so the refused message is newer than everything in it, and everything
// read alongside it is still good.
func (b *Bridge) requestCatchUpForHoleLocked() {
	b.holeEpoch++
	b.requestCatchUpLocked()
}

// requestCatchUpForRefusalLocked asks for the window to be re-read because a
// queue was full. The caller must hold b.mu.
//
// The stamp moves for every refusal, as it does for every hole. What it buys
// here is smaller and still necessary: a catch-up already in flight went to
// Slack before this message was refused, so it may hand over everything it
// read — the refused message is newer than all of it — but it may not clear the
// flag, or nobody would go back for the one that did not fit.
func (b *Bridge) requestCatchUpForRefusalLocked() {
	b.requestCatchUpLocked()
}

// waitForPump waits for a pump to finish, briefly. Nothing about shutdown
// should hang on it: the wait exists so a cursor being recorded reaches the
// writer before the writer is told to stop, and that is the work of a moment.
func waitForPump(done <-chan struct{}) {
	if done == nil {
		return
	}
	timeout := time.NewTimer(pumpStopWait)
	defer timeout.Stop()
	select {
	case <-done:
	case <-timeout.C:
		log.Printf("gave up waiting for the connection reader to stop")
	}
}

// pumpStopWait is how long shutdown waits for the pump to notice it is over.
const pumpStopWait = 2 * time.Second

// stopConnectionLocked ends the connection being replaced, if there is one: its
// pump, and the Socket Mode goroutines under it. The caller must hold b.mu.
//
// It does not wait for any of them to notice. What matters is that nothing is
// left reading a stream nobody is using, or holding a WebSocket open on a
// connection that has been replaced.
func (b *Bridge) stopConnectionLocked() {
	if b.stopConnection == nil {
		return
	}
	b.stopConnection()
	b.stopConnection = nil
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
	// Nothing is listening after this, so nothing should be reading the socket
	// either — nor holding it open. The generation moves with it, so a call
	// still in flight commits nothing on the way out: its cursor would be
	// written by a writer that is stopping, and a cursor that moves without
	// being written is messages skipped after a restart.
	b.stopConnectionLocked()
	b.connGeneration++
	// Terminal from here. A call still running could otherwise reach ensure
	// after this, open a connection, and leave a pump reading into a bridge
	// whose state writer has stopped.
	b.closed = true
	pumpStopped := b.pumpDone
	b.stopIndicatorLocked()
	done := b.indicatorDone
	lock := b.lock
	b.lock = nil
	// Whatever the counts said, nobody is listening once this returns. Saying so
	// explicitly matters because the process is about to exit: a file left
	// reading "one wait" would tell the Stop hook the attendant is fine when it
	// is gone.
	b.activeWaits, b.activeAsks = 0, 0
	presence := b.presenceLocked()
	b.mu.Unlock()

	b.writePresence(presence)

	// The pump first, then the writer it hands cursors to. In that order,
	// because a pump still running can record one more — and a writer already
	// stopped would leave it in a queue nobody is reading, which is a
	// conversation the owner has to open again after a restart.
	waitForPump(pumpStopped)
	writerStopped, mayReleaseLock := b.stopStateWriter()

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
	if !writerStopped {
		// The writer has been fenced: it will not write again, whatever it was
		// holding. Said plainly because what it was holding is now work to be
		// done again after a restart.
		log.Printf("the state file writer was stopped before it finished; what it had not written will be read again after a restart")
	}
	if !mayReleaseLock {
		// A write is still inside the store and cannot be called back. The
		// lock is what keeps the next session from reading a file this one can
		// still rewrite, so it is held until that write is over — and released
		// from there rather than here, because an open file nobody refers to
		// any more is closed by the runtime, and closing it releases the very
		// lock being held.
		go b.releaseWhenWritesEnd(lock)
		return nil
	}
	return lock.Release()
}

// lockHoldReport is how long a lock is held for a write before the wait is
// worth a line in the log. It is not a deadline: letting go while that write is
// still running is the one thing the lock is being held for.
const lockHoldReport = time.Minute

// releaseWhenWritesEnd holds the single-instance lock until the write that
// outlasted shutdown has finished, and only then lets it go. The reference
// matters as much as the timing: an os.File that becomes unreachable is closed
// by the runtime, and the close releases the lock.
func (b *Bridge) releaseWhenWritesEnd(lock *Lock) {
	// No deadline. A write still inside the store can rename the file at any
	// moment, and the lock is the only thing standing between that rename and
	// the session that would otherwise have taken this file over: giving up on
	// the wait gives up on the guarantee. The wait ends when the write does,
	// and if it never does the process is going nowhere either.
	reported := false
	started := time.Now()
	for {
		b.stateWriteMu.Lock()
		writing := b.stateWriting
		b.stateWriteMu.Unlock()
		if !writing {
			break
		}
		if !reported && time.Since(started) > lockHoldReport {
			reported = true
			log.Printf("a state file write has been running for over a minute; the single-instance lock is being held until it ends, so another session cannot start against this directory")
		}
		time.Sleep(stateWriteIdlePoll)
	}
	if err := lock.Release(); err != nil {
		log.Printf("could not release the single-instance lock after the last state file write: %s", logSafe(err.Error(), maxLoggedError))
	}
}

// ensure performs the lazy connect: validate configuration, take the
// single-instance lock, load the persisted cursor, and open Slack. It is
// idempotent. The caller must hold b.mu.
func (b *Bridge) ensure() error {
	if b.closed {
		return errors.New("the bridge is shut down")
	}
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

		// A cursor is one way of knowing the channel has been looked at; the
		// mark is the other, and the only one an empty channel leaves. Without
		// it a restart would seed again and take the first message posted
		// while the session was down for the channel's past.
		seeded, err := b.store.Seeded(b.cfg.Channel)
		if err != nil {
			return err
		}
		b.cursorSeeded = lastTS != "" || seeded

		mentionCursor, err := b.store.MentionCursor()
		if err != nil {
			return err
		}
		b.mentionCursor = mentionCursor

		if err := b.loadThreadsLocked(); err != nil {
			return err
		}
	}

	// The connection gets a context of its own, so that stopping it stops
	// everything it started: the pump here, and the Socket Mode goroutines the
	// connector runs. Bounded by the session's context, so the session ending
	// still ends all of it.
	// The old connection stops being the live one before it is cancelled, not
	// after the replacement is open. Cancelling does not stop a pump the
	// instant it is called, and a Connect that fails leaves no replacement to
	// take over: between the two, an old pump that was still going would have
	// been applying events as the live connection, and a catch-up in flight on
	// it would have found itself current and handed back the cancellation of a
	// connection nobody was waiting on any more.
	if b.stopConnection != nil {
		b.connGeneration++
	}
	b.stopConnectionLocked()
	connCtx, stopConnection := context.WithCancel(b.ctx)

	api, stream, err := b.connector.Connect(connCtx, b.cfg)
	if err != nil {
		stopConnection()
		return err
	}
	b.stopConnection = stopConnection
	b.connCtx = connCtx
	// This connection has not said hello yet, and has thrown nothing away.
	b.connectAnnounced = false
	b.holeDiscards = 0

	b.api = api
	// Its own user ID is how the bridge recognises a mention. Without it the
	// home channel still works and nothing else opens, which is why this is
	// read rather than required.
	b.botUserID = api.BotUserID()
	b.stream = stream
	b.connected = true
	// A fresh connection is the one moment the scopes can have changed, so the
	// catch-up outside the home channel is offered another go — and anything the
	// old connection is still doing no longer speaks for this one. The count is
	// also what every part of this connection is identified by, so it moves
	// before anything is started on it.
	b.connGeneration++
	generation := b.connGeneration

	// One goroutine owns the socket from here. It runs on the connection's
	// context rather than a call's, so a wait that is cancelled does not take
	// the connection down with it — and it stops when the connection does, so
	// two pumps can never write into the same queues.
	pumpDone := make(chan struct{})
	b.pumpDone = pumpDone
	go func() {
		defer close(pumpDone)
		b.pump(connCtx, generation, stream)
	}()
	// The first catch-up covers everything missed since the last session;
	// StreamConnected events later cover reconnects. A hole, because that is
	// what a session that was not running is.
	b.requestCatchUpForHoleLocked()
	return nil
}

// lastLookSlotWait is how long the drain at the deadline will wait for its turn
// at catch-up. The deadline has already passed by then, so this is a courtesy
// rather than a budget: long enough for a slot that is about to come free,
// short enough that the answer is still prompt.
const lastLookSlotWait = 250 * time.Millisecond

// lastLookGrace is how long the drain at the deadline may spend on Slack once
// it has its turn. The deadline has passed by then, so the whole of the last
// look is a courtesy — bounded here so that it stays one.
const lastLookGrace = time.Second

// maxPendingMessages bounds the messages waiting to be handed over. What it
// protects against is a session left working for hours while a busy channel
// fills the heap behind it.
//
// It sits well inside what one catch-up can fetch — maxHistoryPages pages of
// historyPageLimit each — because a refused message is recovered by re-reading
// the window from the cursor, and that window has to hold everything still
// queued as well as everything refused.
const maxPendingMessages = 512

// WaitResult is what slack_wait returns.
type WaitResult struct {
	Messages []Message `json:"messages"`
	// Reactions are the emoji put on or taken off messages the session can
	// see, since the last delivery. The field is absent when there are none,
	// so a caller that only knows about messages reads the same result it
	// always did.
	Reactions []Reaction `json:"reactions,omitempty"`
	// ReactionsDropped reports that at least one reaction was received and
	// could not be queued, so the emoji delivered here are not the whole story.
	// There is no way to recover which: read the tally of anything you are
	// counting with slack_reactions. Absent means nothing was lost.
	ReactionsDropped bool `json:"reactions_dropped,omitempty"`
	TimedOut         bool `json:"timed_out"`
}

// Wait blocks until at least one owner message or one reaction is available, or
// the timeout expires. Either kind on its own ends it, and a wait that has both
// hands over both.
//
// The first call connects and runs catch-up, so a backlog that accumulated
// while the session was down comes back immediately as an array rather than
// trickling in. After that it waits on the live stream, running catch-up again
// on every reconnect. Catch-up is for messages: reactions live only on the
// connection, and the ones missed while it was down are read back with
// Reactions instead.
func (b *Bridge) Wait(ctx context.Context, timeout time.Duration) (WaitResult, error) {
	// Before anything that can fail: the point of the presence file is that
	// somebody is listening, and a wait that ends in an error was still a wait
	// while it lasted.
	b.enterWait()
	defer b.exitWait()

	// Waiting again means the agent is done with whatever it was given last
	// time, even if it never posted a reply.
	b.stopIndicator()

	b.mu.Lock()
	if err := b.ensure(); err != nil {
		b.mu.Unlock()
		return WaitResult{}, err
	}
	generation := b.connGeneration
	b.mu.Unlock()

	// Subscribed before the first drain, so a message absorbed by another call
	// between the drain and the select is a wakeup rather than a message this
	// call blocks straight through.
	sub := b.subscribePending()
	defer b.unsubscribePending(sub)

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	waitEnds := time.Now().Add(timeout)

	// drainWithin runs a catch-up that may not outlive this call's own
	// deadline: neither the wait for its turn, nor the requests it makes once
	// it has one. A fetch cut short that way is not an error and not a loss —
	// nothing is committed until the messages are handed over, so the next
	// call reads the same window — and the empty answer is the one this call
	// promised by that time.
	drainWithin := func(budget, slotWait time.Duration) ([]Message, []Reaction, error) {
		drainCtx, cancelDrain := context.WithTimeout(ctx, budget)
		defer cancelDrain()

		msgs, reactions, err := b.drainCatchUp(drainCtx, generation, true, slotWait)
		if err != nil && ctx.Err() == nil && drainCtx.Err() != nil {
			return nil, nil, nil
		}
		return msgs, reactions, err
	}

	for {
		// Before anything is taken off a queue. A caller that has given up
		// should not consume a batch on its way out: those messages are the
		// owner's, and the next call is the one that will answer them.
		if err := ctx.Err(); err != nil {
			return WaitResult{}, err
		}

		// Catch-up first: a pending backlog outranks waiting for something
		// new, and on a reconnect it is the only place missed messages are.
		// Everything the socket has delivered is already in the queues, put
		// there by the pump, so there is nothing to sweep here.
		// The slot is waited for only as long as this call has left: another
		// call's slow request must not make this one answer late.
		budget := time.Until(waitEnds)
		msgs, drained, err := drainWithin(budget, budget)
		if err != nil {
			return WaitResult{}, err
		}
		if len(msgs) > 0 || len(drained) > 0 {
			return b.deliver(ctx, generation, msgs, drained), nil
		}

		// The caller giving up comes first. Cancelling this call cancels the
		// session's context in the usual arrangement, which ends the pump,
		// which says the connection is gone — and answering "the connection
		// closed" to a caller that has cancelled describes the consequence
		// rather than the cause.
		if err := ctx.Err(); err != nil {
			return WaitResult{}, err
		}
		// Only once there is nothing to hand over. Anything the dying
		// connection had already delivered is in the queues above, and
		// reporting the disconnection before taking it would throw away what
		// the owner actually sent.
		if b.streamGone(generation) {
			return WaitResult{}, errors.New("the Slack connection closed")
		}

		// A loss is news on its own. Without this the wait would go back to
		// sleep on a full-length deadline, holding a marker that says the
		// agent's count is wrong and that nothing else is going to mention.
		if b.takeReactionsDropped(generation) {
			return WaitResult{Messages: []Message{}, ReactionsDropped: true}, nil
		}

		select {
		case <-ctx.Done():
			return WaitResult{}, ctx.Err()

		case <-deadline.C:
			// One last look before giving up. A message can reach the queue at
			// any point during the poll, including the instant the timer fires
			// and including from another call entirely, and reporting an empty
			// timeout on top of one would hold it back for another full poll
			// while the owner waits on a reply.
			if err := ctx.Err(); err != nil {
				return WaitResult{}, err
			}
			msgs, drained, err := drainWithin(lastLookSlotWait+lastLookGrace, lastLookSlotWait)
			if err != nil {
				return WaitResult{}, err
			}
			if len(msgs) > 0 || len(drained) > 0 {
				// Delivered is delivered, however close to the bell it was. A
				// timed_out of true alongside messages would be read as "call
				// again", which is the one thing that must not happen to
				// messages already handed over.
				return b.deliver(ctx, generation, msgs, drained), nil
			}
			// A connection that ended while the last history request was in
			// flight is news, and a quiet timeout would leave it for whoever
			// called next.
			if b.streamGone(generation) {
				return WaitResult{}, errors.New("the Slack connection closed")
			}
			return WaitResult{
				Messages: []Message{},
				// A wait with nothing to hand over still has to say a
				// reaction was lost: that is precisely when the agent's count
				// is wrong and nothing else would tell it.
				ReactionsDropped: b.takeReactionsDropped(generation),
				TimedOut:         true,
			}, nil

		case <-sub:
			// Something reached the queue, or the connection ended. Round the
			// loop, which drains the first and reports the second.
		}
	}
}

// deliver hands a drained batch to the agent: the clock starts, the owner sees
// their messages marked as received, and the batch goes back as a delivery.
//
// It is shared by the two places a wait can hand messages over — the drain at
// the top of the loop and the one guarding the deadline — because they are the
// same event and the owner should not be able to tell which one it came from.
//
// The indicator is started where the owner is looking: the newest message is
// the one they just sent, so its channel and thread are the conversation they
// are waiting on.
func (b *Bridge) deliver(ctx context.Context, generation uint64, msgs []Message, reactions []Reaction) WaitResult {
	// Only messages start the clock. A reaction is not something the owner is
	// waiting on an answer to, and marking one as received would put a receipt
	// emoji on a message for every emoji anybody else put on it.
	if len(msgs) > 0 {
		b.startIndicator(newestConversation(msgs))
		b.autoAck(msgs)
	}
	if msgs == nil {
		// Never null. A reaction-only delivery has no messages, and a caller
		// reading the bridge directly gets the empty array every other result
		// has always carried.
		msgs = []Message{}
	}
	return WaitResult{
		Messages:         msgs,
		Reactions:        b.nameReactions(ctx, reactions),
		ReactionsDropped: b.takeReactionsDropped(generation),
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
func (b *Bridge) noteStreamClosed(generation uint64, stream Stream) {
	// Whatever this connection lost is still the agent's to hear about, and the
	// stream that recorded it is going away. It is taken here rather than in
	// any one caller because the socket closes its channels together and a call
	// can notice any of them first: this is the one place every disconnect
	// passes through. It is taken even when the stream has already been
	// replaced — the loss happened either way.
	// The stream that is closing, not the one the bridge holds: a connection
	// that has already been replaced still lost what it lost, and its marker
	// has nowhere else to go. Asked outside the lock, because it is a question
	// for somebody else's implementation.
	dropped := streamDroppedReactions(stream)

	b.mu.Lock()
	defer b.mu.Unlock()

	if dropped {
		b.reactionsDropped = true
	}
	if !b.stale(generation) {
		b.connected = false
	}
	// A blocked call is waiting for something to happen, and this is something:
	// it rounds its loop, finds the queues empty and the connection gone, and
	// says so instead of sitting out its whole timeout on a dead socket.
	b.notifyPendingLocked()
}

// absorbLocked folds one stream event into the bridge's pending state. The
// caller must hold b.mu, which is how a reaction and the messages ahead of it
// are applied as one step.
//
// The pump is the only caller: it applies what the socket delivers, and the
// queues it writes are what slack_wait and slack_ask read. Nothing here decides
// who receives a message — only where it waits until somebody does.
func (b *Bridge) absorbLocked(evt StreamEvent) {
	switch evt.Kind {
	case StreamMessage:
		// The socket relays every owner message in every channel the bot is in,
		// because which conversations are open changes while the session runs.
		// This is where that is decided.
		msg, ok := b.classifyLocked(evt.Message)
		if !ok {
			return
		}
		if b.alreadyDeliveredLocked(msg) {
			// The same message twice. Slack retries an envelope it has not
			// been acknowledged for, and the receipt can be lost after the
			// message itself arrived — so this is the owner's message coming
			// round again, not a second one.
			return
		}
		// The socket's buffer used to be the limit, because nothing moved a
		// message off it until a call asked; the pump moves every one, so the
		// limit has to be here instead.
		//
		// What is already queued stays. Dropping it would throw away messages
		// that were received, and the newest is the one history is most
		// certain to still have. The two queues are counted apart so that a
		// flood in the home channel cannot crowd out a reply in a conversation
		// elsewhere: those are the ones history is least able to give back,
		// since that catch-up is best effort and stands down entirely when a
		// scope is missing.
		home := msg.Channel == "" || msg.Channel == b.cfg.Channel
		queue, full := &b.pendingThreads, &b.threadsFull
		if home {
			queue, full = &b.pending, &b.pendingFull
		}
		if len(*queue) >= maxPendingMessages {
			b.requestCatchUpForRefusalLocked()
			if home && !b.cursorSeeded {
				// Refused before the home cursor exists. The seed about to be
				// established would filter this message out as older than
				// itself, so the seed is given up on instead: the cursor is
				// taken from the messages actually handed over, and catch-up
				// reads everything after them.
				//
				// Only for the home channel. A reply queued elsewhere has
				// nothing to do with that cursor, and giving up the seed for
				// one would replay the home channel's history as new.
				b.preSeedRefused = true
			}
			// One line per episode, not one per message. This runs under the
			// lock the pump holds, and a flood that logged every refusal would
			// turn the overflow into the thing that stopped the socket.
			if !*full {
				*full = true
				log.Printf("a pending message queue is full at %d; further messages will be re-read from history", maxPendingMessages)
			}
			return
		}
		// Cleared for this queue only: the two fill and drain independently,
		// and one of them taking a message says nothing about the other.
		*full = false
		*queue = append(*queue, msg)
		b.notifyPendingLocked()
	case StreamDropped:
		// The stream announcing a message it refused. If the pump already saw
		// that refusal by asking — it polls, because the announcement needs
		// room on the very channel that had none — this is the same hole
		// arriving a second time, and raising another would throw away a
		// second catch-up for one lost message.
		if b.overflowNoted.Swap(false) {
			return
		}
		b.requestCatchUpForHoleLocked()

	case StreamConnected:
		// Every connection announces itself once, and the catch-up for that is
		// asked for where the connection is opened — so this first hello is
		// that request arriving, not a reconnect on top of it. Raising a hole
		// here would discard the first catch-up of every session, every time.
		//
		// Only while that catch-up is still outstanding. A hello that arrives
		// after it has run is not the one it was asked for, and the safe
		// reading of a connection announcing itself is that something was
		// missed.
		firstHello := !b.connectAnnounced
		b.connectAnnounced = true
		if firstHello && b.needCatchUp {
			return
		}
		// A reconnect underneath a connection that was never replaced — the
		// socket library's own — means the live stream may have a hole in it.
		// History is the authority, so go re-read the window after the cursor.
		b.requestCatchUpForHoleLocked()
	}
}

// drainCatchUp runs catch-up when it is due, merges the result with anything
// already pending, and hands the messages over.
//
// The cursor is advanced and persisted only once the messages are about to be
// returned, so a failure anywhere earlier leaves the bridge ready to fetch
// them again on the next call rather than skipping past them.
func (b *Bridge) drainCatchUp(ctx context.Context, generation uint64, takeReactions bool, slotWait time.Duration) ([]Message, []Reaction, error) {
	// One at a time. Two calls reading the same window would both fetch it and,
	// behind a gap — where the cursor deliberately stays put — both deliver it,
	// handing the owner's messages over twice. The wait costs nothing that was
	// not going to be spent: the second call was about to make the same
	// request, and by the time it has its turn the first has moved the cursor
	// past what it took.
	//
	// A caller that gives up while waiting says so rather than waiting out
	// somebody else's slow request: its own deadline is the one it promised.
	wait := time.NewTimer(slotWait)
	defer wait.Stop()
	select {
	case b.catchUpSlot <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-wait.C:
		// Somebody else's request is still out, and this call has its own
		// deadline to keep. It hands back what that deadline asked for —
		// nothing — rather than answering late on the strength of a fetch it
		// never made.
		return nil, nil, nil
	}
	defer func() { <-b.catchUpSlot }()

	b.mu.Lock()
	if b.stale(generation) {
		// The connection this call belongs to has been replaced while it
		// waited for its turn. Everything it read would be thrown away at the
		// commit below, and reading it would hold the slot the replacement's
		// own catch-up is waiting for.
		b.mu.Unlock()
		return nil, nil, nil
	}
	needCatchUp := b.needCatchUp
	// Threads left unread ask for a walk of their own. It is a smaller errand
	// than a catch-up: no window, no scan, only the conversations that were
	// skipped.
	threadsOnly := !needCatchUp && b.threadsSkipped
	epoch := b.catchUpEpoch
	holes := b.holeEpoch
	seeded := b.cursorSeeded
	api := b.api
	lastTS := b.lastTS
	channel := b.cfg.Channel
	owner := b.cfg.Owner
	connCtx := b.connCtx
	b.mu.Unlock()

	// The fetch follows the connection as well as its caller. What it reads on
	// a connection that has been replaced is committed nowhere — the check
	// below throws all of it away — and until it returns it holds the slot the
	// new connection's own catch-up is waiting for. Ending it with the
	// connection is what keeps one slow request on a dead connection from
	// standing in front of the live one.
	ctx, endFetch := context.WithCancel(ctx)
	defer endFetch()
	if connCtx != nil {
		defer context.AfterFunc(connCtx, endFetch)()
	}

	// A fetch that was cut short this way has nothing to report: the caller's
	// own context is still good, and the answer to a connection that ended
	// underneath it is the empty one its replacement will fill in.
	abandoned := func(err error) bool {
		if !errors.Is(err, context.Canceled) || ctx.Err() == nil {
			return false
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.stale(generation)
	}

	// What the scan outside the home channel would change is staged here and
	// committed below, under the same generation check as everything else.
	scan := &scanChanges{}

	var (
		fetched, conversations []Message
		// seeding marks the first run against a channel, where the cursor is
		// being established rather than read from.
		seeding bool
	)
	if threadsOnly {
		var err error
		conversations, err = b.catchUpSkippedThreads(ctx, api, owner, generation, scan)
		if err != nil {
			if abandoned(err) {
				return nil, nil, nil
			}
			return nil, nil, err
		}
	}

	if needCatchUp {
		if !seeded {
			// First run against this channel: seeding from the newest
			// message means a fresh install starts a conversation rather
			// than replaying the channel's entire history into the agent.
			seeded, err := b.seedCursor(ctx, api, generation, channel)
			if err != nil {
				if abandoned(err) {
					return nil, nil, nil
				}
				return nil, nil, err
			}
			lastTS = seeded
			seeding = true
		} else {
			var (
				err       error
				truncated bool
			)
			fetched, truncated, err = catchUp(ctx, api, channel, owner, lastTS)
			if err != nil {
				if abandoned(err) {
					return nil, nil, nil
				}
				return nil, nil, err
			}
			if truncated {
				// More window than one pass can read, and the pages it read
				// are the newest of it: what was not reached is older than
				// everything delivered, so no later pass can get back to it —
				// asking for one would only read these same pages again.
				//
				// This is the bound the design has always had on how far back
				// a single catch-up will go. What is new is saying so.
				log.Printf("catch-up read the newest %d messages and stopped; anything older in the window was not delivered, including messages refused for want of room while the flood lasted",
					maxHistoryPages*historyPageLimit)
			}
		}

		// Everywhere else: the conversations opened by a mention, and any
		// mention that opened one while nobody was listening.
		var err error
		conversations, err = b.catchUpConversations(ctx, api, owner, generation, scan)
		if err != nil {
			if abandoned(err) {
				return nil, nil, nil
			}
			return nil, nil, err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// A drain that started on a connection since replaced commits nothing at
	// all. What it read came from the installation as it was, and the queues it
	// would merge are being filled by the connection that replaced it: taking
	// them here hands the new connection's messages to a call that is about to
	// be told its own connection is gone, and leaves the cursor where it was so
	// they come back twice. Worse for a reply in a conversation outside the
	// home channel, where the catch-up that would find it again is best effort.
	//
	// The replacement asks for its own catch-up on connect, and that one reads
	// the window with the installation as it now is.
	if generation != b.connGeneration {
		// One exception, and it commits nothing: a cursor that has never been
		// set. The seed says where this session found the channel, and the
		// replacement would otherwise seed again against a channel that has
		// moved on — treating everything sent in between as history and
		// delivering none of it. Kept in memory only; the write belongs to the
		// call that hands the messages over.
		if seeding && !b.cursorSeeded && !b.closed {
			b.establishSeedLocked(channel, lastTS, false)
			// Nothing here may write, and the call that takes over may have
			// nothing to hand over — an empty home channel, or replies and no
			// messages — in which case it commits no cursor of its own. The
			// debt is recorded so that whichever call commits next pays it:
			// unpaid, a restart seeds again and reads everything sent while
			// this session was down as the channel's past.
			b.seedUnwritten = true
		}
		return nil, nil, nil
	}

	// A hole opened while this was reading. Nobody knows where it is, so
	// nothing read here can be trusted to belong after it: the lot is thrown
	// away and the request stands for the next call, which reads the window
	// with the hole already in the past.
	//
	// It costs one round trip, and it is the whole of the rule. What replaced
	// it — delivering the fetch while holding the cursor back over a queue
	// that was kept — handed the same messages over again on every pass, and
	// under a queue that stayed full it never stopped.
	if holes != b.holeEpoch {
		// Twice over is a storm, not an accident. A socket that flaps faster
		// than a catch-up takes would otherwise keep throwing every pass away,
		// and a message the owner sent — sitting in the queue, already
		// received — would wait for the flapping to stop.
		//
		// So the second discard in a row hands over the queue and nothing
		// else. Those messages came off the socket, which is what makes them
		// safe: whatever the hole swallowed, it did not swallow these. What
		// was fetched is still dropped, and the cursor still does not move, so
		// nothing steps over the hole — the next pass reads that window again
		// and the delivered window keeps it from arriving twice.
		if b.holeDiscards > 0 && len(b.pending)+len(b.pendingThreads) > 0 {
			b.holeDiscards = 0
			return b.handOverQueuesLocked(takeReactions)
		}
		b.holeDiscards++

		if seeding && !b.cursorSeeded && !b.closed {
			// The seed is not part of what was read; it is where the channel
			// was when this session found it, which no hole changes. Kept, so
			// the next pass does not seed again against a channel that has
			// moved on — in memory, with the write left to whoever commits.
			b.establishSeedLocked(channel, lastTS, false)
			b.seedUnwritten = true
		}
		return nil, nil, nil
	}

	if needCatchUp || threadsOnly {
		// What the scan found, under this check like everything else: a
		// conversation it opened, one it gave up on, and how far it looked are
		// all true of the installation it ran with.
		b.commitScanLocked(scan)

		// The cursor first, and whatever else has been asked for since. A seed
		// is a fact about the channel — where it was when this session found
		// it — and dropping it because something asked for another catch-up
		// meanwhile would leave the cursor unset, to be seeded again against a
		// channel that has moved on.
		if seeding && !b.cursorSeeded {
			b.establishSeedLocked(channel, lastTS, true)
		}
		// The flag, only if nothing has asked again since this one started. A
		// message refused while history was in flight is a request this
		// catch-up never saw: what it read is still good — the refused message
		// is newer than everything queued, so the cursor may move over the
		// queue without stepping past it — but the refused message itself is
		// only in the window, and clearing the flag would leave nobody to
		// fetch it.
		//
		// The epoch is compared again here, not just at the top: establishing a
		// seed over a refusal asks for another pass of its own, and clearing
		// the flag on the way past would answer that request with this window.
		if epoch == b.catchUpEpoch {
			b.needCatchUp = false
		}
		// Finished, whether or not it cleared the flag: the window was read,
		// which is what a reaction waiting for an explanation was waiting for.
		b.catchUpRuns++
	}

	// Threads the walk could not reach. Recorded on their own flag, which asks
	// for another walk and nothing else.
	if needCatchUp || threadsOnly {
		b.threadsSkipped = scan.skipped
		b.skippedThreads = nil
		if len(scan.skippedKeys) > 0 {
			b.skippedThreads = make(map[threadKey]struct{}, len(scan.skippedKeys))
			for _, key := range scan.skippedKeys {
				b.skippedThreads[key] = struct{}{}
			}
		}
	}

	// A pass that reached here is a pass that was not thrown away.
	b.holeDiscards = 0

	// The seed another call established in memory and could not write. Paid
	// here, where the connection is the live one and the lock is held, and
	// before any of the early returns below.
	b.writeSeedDebtLocked(channel)

	// Live events and history overlap around a reconnect; merging deduplicates
	// by timestamp. Only what history returned is filtered by the cursor: a
	// message the socket delivered is not the channel's past, whatever its
	// timestamp says, and on the run that seeds the cursor it can be older
	// than the seed and still be the message this session was started for.
	live, liveThreads := b.pending, b.pendingThreads

	// What has already been handed over is not fetched again. The cursor
	// catches most of it, but a storm hands the queue over without moving one
	// — so history returns those messages on the next pass, and this is what
	// stops them arriving a second time.
	read, readThreads := fetched, conversations
	fetched = b.undeliveredLocked(fetched)
	conversations = b.undeliveredLocked(conversations)

	home := mergeLive(b.lastTS, fetched, live)
	threads := b.mergeThreadMessagesLocked(conversations, liveThreads)
	b.pending = nil
	b.pendingThreads = nil

	// Taken here, under the same lock, rather than by a second call. Two waits
	// running together could otherwise split a pair the pump applied as one:
	// the first takes the message and yields, the second takes the reaction
	// that came with it, and each hands over half.
	//
	// Only for a caller that delivers them. A question collects the messages
	// that arrived while it was up and returns them with its answer, but it
	// has nowhere to put a reaction — taking one here would be taking it off
	// the wait that reports them.
	var reactions []Reaction
	if takeReactions {
		reactions = b.drainReactionsLocked()
	}

	// Remembered before it goes, so a redelivered envelope is recognised as
	// the message it already is.
	b.noteDeliveredLocked(home, threads)

	// The cursor moves over everything this pass read, not only over what it
	// handed on. Nothing is being held back behind it — a hole would have
	// thrown the whole pass away above, and a refusal leaves nothing older
	// than the queue unread — and what it read but did not hand on was
	// dropped for one reason: it had been handed over already.
	//
	// The distinction is the storm's. A pass that hands the queue over without
	// moving the cursor leaves those messages in the window, and the pass that
	// comes after finds them there and drops them as delivered; taking the
	// cursor from what survived that would leave it behind them for good.
	newest := newestTS(home)
	if last := newestTS(read); tsLess(newest, last) {
		newest = last
	}

	if len(home) == 0 && len(threads) == 0 && newest == "" {
		return nil, reactions, nil
	}

	if newest != "" {
		// Never backwards. On the run that seeds the cursor, a message the pump
		// took while history was being read can be older than the seed, and
		// moving the cursor back to it would have the next reconnect re-read
		// the channel's past.
		if tsLess(newest, b.lastTS) {
			newest = b.lastTS
		}
		if b.store != nil {
			// A stale cursor costs a duplicate after a restart; the messages
			// reach the agent either way.
			b.recordStateWriteLocked(stateWrite{
				stateKey: stateKey{kind: writeLastTS, channel: channel},
				ts:       newest,
			})
		}
		b.lastTS = newest
	}
	// A conversation the walk could not reach keeps its cursor, whatever this
	// pass hands over from it. The socket can deliver a reply in a conversation
	// the walk skipped, and moving the cursor to that reply steps over
	// everything the walk was going to go back for — which is the promise the
	// skipped list makes. The reply is still handed over; only the cursor
	// waits, and the walk that reads that conversation moves it.
	missed := make(map[threadKey]struct{}, len(scan.skippedKeys))
	for _, key := range scan.skippedKeys {
		missed[key] = struct{}{}
	}
	reached := func(m Message) bool {
		_, skipped := missed[threadKey{m.Channel, m.ThreadTS}]
		return !skipped
	}

	for _, m := range threads {
		if reached(m) {
			b.noteThreadDeliveredLocked(m)
		}
	}
	// The replies this pass read and did not hand on, for the same reason: they
	// were handed on by the pass that gave up in the storm, and a cursor left
	// behind them would fetch them again on every hole.
	for _, m := range readThreads {
		if reached(m) && b.alreadyDeliveredLocked(m) {
			b.noteThreadDeliveredLocked(m)
		}
	}

	if len(home) == 0 && len(threads) == 0 {
		return nil, reactions, nil
	}
	return mergeConversations(home, threads), reactions, nil
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
	b.recordStateWriteLocked(stateWrite{
		stateKey: stateKey{kind: writeThread, channel: m.Channel, threadTS: m.ThreadTS},
		ts:       m.TS,
	})
}

// oldestPendingLocked reports the timestamp of the oldest home message waiting
// to be handed over, or empty if there is none. The caller must hold b.mu.
func (b *Bridge) oldestPendingLocked() string {
	oldest := ""
	for _, m := range b.pending {
		if oldest == "" || tsLess(m.TS, oldest) {
			oldest = m.TS
		}
	}
	return oldest
}

// handOverQueuesLocked hands over what the socket has already delivered,
// without moving any cursor. The caller must hold b.mu.
//
// It is the way out of a storm. Nothing here was fetched, so nothing here can
// be on the far side of the hole that keeps interrupting: these messages came
// off this connection, into these queues, before the pass that is giving up
// looked at them. The cursors stay where they are, which means the window will
// be read again — and what it returns will have been handed over already, which
// is what the delivered window is for.
func (b *Bridge) handOverQueuesLocked(takeReactions bool) ([]Message, []Reaction, error) {
	home := mergeMessages("", b.pending)
	threads := b.mergeThreadMessagesLocked(b.pendingThreads)
	b.pending = nil
	b.pendingThreads = nil

	var reactions []Reaction
	if takeReactions {
		reactions = b.drainReactionsLocked()
	}

	// No cursor moves here, the thread ones included: a hole of unknown
	// position may sit behind a reply older than the newest of these, and a
	// cursor stepped over it is a reply nobody ever receives. The window comes
	// round again, and the delivered window is what keeps it from arriving
	// twice.
	b.noteDeliveredLocked(home, threads)
	if len(home) == 0 && len(threads) == 0 {
		return nil, reactions, nil
	}
	return mergeConversations(home, threads), reactions, nil
}

// writeSeedDebtLocked records a seed that was established in memory by a call
// that could not write it. The caller must hold b.mu and must be on the live
// connection.
func (b *Bridge) writeSeedDebtLocked(channel string) {
	if !b.seedUnwritten || b.store == nil || b.closed {
		return
	}
	b.seedUnwritten = false

	if b.lastTS == "" {
		// An empty channel leaves no cursor, only the mark that says it was
		// looked at.
		b.recordStateWriteLocked(stateWrite{
			stateKey: stateKey{kind: writeSeeded, channel: channel},
		})
		return
	}
	b.recordStateWriteLocked(stateWrite{
		stateKey: stateKey{kind: writeLastTS, channel: channel},
		ts:       b.lastTS,
	})
}

// establishSeedLocked records where this session found the channel. The caller
// must hold b.mu.
//
// The seed is the cursor, and the messages taken from the socket while it was
// being read are not filtered by it: they arrived after the session started,
// and they are older than a mark that describes the past. That is what the
// merge's rule about live messages is for. Without the seed as the cursor,
// catch-up would have no bound and would read the channel's whole history back
// as new.
//
// commit is false for a call that can no longer deliver: the cursor is kept in
// memory so a replacement does not seed again against a channel that has moved
// on, and the write is left to whoever hands the messages over.
func (b *Bridge) establishSeedLocked(channel, seed string, commit bool) {
	if b.preSeedRefused {
		// The refused messages are only recoverable by reading the window
		// again, so the request for that must outlive this call: the drain
		// that seeded is the one that would otherwise clear it. A refusal
		// rather than a hole — what this pass read is still good, and the
		// cursor it is about to establish is behind the refused messages.
		b.needCatchUp = true
		b.catchUpEpoch++

		// The queue filled while the seed was being read, so messages were
		// refused. They are newer than everything queued, and the queue is
		// what the session has actually seen — so the cursor goes behind the
		// oldest of those instead of on the seed, and catch-up reads forward
		// from there. Nothing before this session is in that window: the
		// oldest queued message arrived on this connection.
		if oldest := b.oldestPendingLocked(); oldest != "" {
			seed = predecessorTS(oldest)
		}
	}

	b.cursorSeeded = true
	b.lastTS = seed
	b.preSeedRefused = false

	if !commit {
		// This call cannot write: what it found is kept in memory, and the
		// call that hands the messages over records it.
		return
	}
	if seed == "" {
		// The channel was empty, so there is no cursor to write — but that it
		// was looked at is worth recording all the same. Without it a restart
		// would seed again, and the first message posted in the meantime would
		// be read as the channel's past and never delivered.
		b.recordStateWriteLocked(stateWrite{
			stateKey: stateKey{kind: writeSeeded, channel: channel},
		})
		return
	}
	// Written here rather than where it was read, so it cannot reach the file
	// ahead of the messages that arrived while it was being read — those are
	// older than the seed and a restart would filter them out.
	b.recordStateWriteLocked(stateWrite{
		stateKey: stateKey{kind: writeLastTS, channel: channel},
		ts:       seed,
	})
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
func (b *Bridge) seedCursor(ctx context.Context, api API, generation uint64, channel string) (string, error) {
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
	// Deliberately not written here. State writes are asynchronous, and a seed
	// that reached the file before the messages queued during it were handed
	// over would, after a crash, filter exactly those messages out as older
	// than the cursor. The call that commits the batch writes it.
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
func catchUp(ctx context.Context, api API, channel, owner, after string) ([]Message, bool, error) {
	if api == nil {
		return nil, false, errors.New("the bridge is not connected to Slack")
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
			return nil, false, err
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
		return nil, false, err
	}

	// A cursor left over means the walk stopped at its page bound with more
	// window behind it. The caller is told so it can come back: the cursor
	// moves only through what was delivered, so another pass continues from
	// there rather than starting again.
	// History pages arrive newest-first; mergeMessages sorts and deduplicates.
	return mergeMessages(after, messages, replies), cursor != "", nil
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
//
// The text goes out as standard Markdown in a markdown block, which Slack
// renders, so a reply is not delivered with its asterisks showing. Past what
// a block holds it goes as the message body instead, where Slack applies its
// own mrkdwn and nothing else — whole, but with its Markdown showing.
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
