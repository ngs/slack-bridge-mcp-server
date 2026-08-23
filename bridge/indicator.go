package bridge

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Indicator timing. The grace period is what keeps the channel quiet for the
// common case: most answers come back in a few seconds, and a "working…" note
// that appears and disappears before it can be read is pure noise. The interval
// is bounded on the low end because every tick is a chat.update, and Slack
// rate-limits that per channel.
const (
	DefaultIndicatorGrace = 10 * time.Second
	MinIndicatorGrace     = 3 * time.Second
	MaxIndicatorGrace     = 120 * time.Second

	DefaultIndicatorInterval = 10 * time.Second
	MinIndicatorInterval     = 5 * time.Second
	MaxIndicatorInterval     = 60 * time.Second
)

// indicatorDeleteTimeout bounds the best-effort chat.delete the indicator makes
// on its way out. It runs on a context of its own because the usual reason the
// indicator is torn down at shutdown is that the session's context was just
// cancelled, and the message would then be left behind in the channel forever.
const indicatorDeleteTimeout = 5 * time.Second

// indicatorRequestTimeout bounds each post and update the indicator makes.
// Without it a Slack call that never answers would hold the goroutine open
// forever, and with it the indicator's done channel — which is what the next
// indicator waits on before posting, and what Close waits on before the
// process exits. A hung request must cost this much and no more.
const indicatorRequestTimeout = 10 * time.Second

// indicator is the "⏳ Working… (1m 05s)" message the owner sees while the
// agent is busy with what slack_wait handed it.
//
// One goroutine owns the whole lifecycle: it sleeps out the grace period, posts
// once, updates in place on a ticker, and deletes the message when it is told
// to stop. Keeping the Slack calls in that goroutine is what lets stop() be a
// non-blocking channel close, so a reply is never held up by the indicator's
// own bookkeeping.
type indicator struct {
	api     API
	channel string
	// threadTS is the thread the turn is happening in, empty when the owner
	// spoke on the channel surface. The indicator belongs wherever the
	// conversation is: a "⏳ Working…" posted to the channel while the owner is
	// talking inside a thread is both noise out there and silence in here.
	// Only the post needs it — chat.update and chat.delete address the message
	// by its own ts, which identifies it whichever surface it sits on.
	threadTS string
	ctx      context.Context
	grace    time.Duration
	interval time.Duration
	// startedAt is when the agent received the messages, which is the elapsed
	// time the owner cares about — not when the first message was posted.
	startedAt time.Time

	// predecessor is the done channel of the indicator this one replaces, if
	// any. Waiting for it before posting is what keeps two indicator messages
	// from being visible at once when the outgoing chat.delete is slow.
	predecessor <-chan struct{}

	// labelMu guards label and postedTS, both of which the goroutine writes or
	// reads while a tool call may be touching them from outside.
	labelMu sync.Mutex
	// label is what slack_progress last said the agent is waiting on, shown
	// next to the elapsed time. Empty until the agent says something.
	label string
	// postedTS is the message the indicator is updating, once it exists.
	postedTS string

	// nudge asks the goroutine to show what has just changed: it cuts the grace
	// period short before the first post and forces an update in place after
	// it. One slot is enough, because a second signal would only repeat what
	// the first already said.
	nudge chan struct{}

	stopOnce sync.Once
	stopped  chan struct{}
	// done is closed once the goroutine has finished, deletion included.
	done chan struct{}
}

func newIndicator(ctx context.Context, api API, channel, threadTS string, grace, interval time.Duration, predecessor <-chan struct{}) *indicator {
	return &indicator{
		api:         api,
		channel:     channel,
		threadTS:    threadTS,
		ctx:         ctx,
		grace:       grace,
		interval:    interval,
		startedAt:   time.Now(),
		predecessor: predecessor,
		nudge:       make(chan struct{}, 1),
		stopped:     make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// start launches the goroutine. ctx must be the bridge's long-lived context,
// not a per-tool-call one: the tool call that starts the indicator returns
// immediately, and its context is cancelled right after.
func (in *indicator) start() {
	go in.run()
}

// stop is idempotent and safe from any goroutine. It returns as soon as the
// goroutine has been told to wind down; the chat.delete happens there.
func (in *indicator) stop() {
	in.stopOnce.Do(func() { close(in.stopped) })
}

func (in *indicator) run() {
	// Closed last, after any deletion below, so that whoever replaces this
	// indicator can tell when the channel is clear again.
	defer in.finish()

	grace := time.NewTimer(in.grace)
	defer grace.Stop()

	select {
	case <-in.stopped:
		// Answered inside the grace period, which is the case worth keeping
		// silent: nothing was ever posted, so there is nothing to clean up.
		return
	case <-in.ctx.Done():
		return
	case <-in.nudge:
		// The agent has said this will take a while, so the silence the grace
		// period is protecting has nothing left to protect.
	case <-grace.C:
	}

	// A nudge and a stop can be ready in the same instant, and select picks
	// between ready cases at random. The stop wins: a turn that has ended must
	// not put a message in the channel, however recently it was labelled.
	if in.stopping() {
		return
	}

	// The indicator this one replaces may still be deleting its message —
	// chat.delete is allowed several seconds, which can outlast a short grace
	// period. Posting before it finishes would put two indicators in the
	// channel, so wait it out here, on this goroutine, rather than making the
	// tool call that started us pay for it.
	if in.predecessor != nil {
		select {
		case <-in.predecessor:
		case <-in.stopped:
			return
		case <-in.ctx.Done():
			return
		}
	}

	ts, err := in.post()
	if err != nil {
		// Without a message there is nothing to update or delete, so give up
		// on this round rather than retrying into a rate limit. The error text
		// comes back from Slack, so it goes through logSafe like any other
		// value the bridge did not write itself.
		log.Printf("could not post the processing indicator: %s", logSafe(err.Error(), maxLoggedError))
		return
	}
	defer in.remove(ts)
	in.notePosted(ts)

	ticker := time.NewTicker(in.interval)
	defer ticker.Stop()

	for {
		select {
		case <-in.stopped:
			return
		case <-in.ctx.Done():
			return
		case <-in.nudge:
			// A new label should not have to wait out the tick interval: the
			// owner asked what is being waited on, and the answer is here now.
			in.tick(ts)
		case <-ticker.C:
			in.tick(ts)

		}
	}
}

// stopping reports that the indicator has been told to wind down, or that the
// session is going away. It is the tie-breaker wherever a stop can be ready at
// the same moment as something else, since select does not rank its cases.
func (in *indicator) stopping() bool {
	select {
	case <-in.stopped:
		return true
	case <-in.ctx.Done():
		return true
	default:
		return false
	}
}

// tick redraws the message, and treats a failure as nothing worse than a stale
// line: one bad update is not worth tearing the indicator down, and the next
// one may well land.
//
// A tick that finds the indicator already stopped does nothing: the message is
// about to be deleted, and an update racing the delete is one Slack call that
// can only fail.
func (in *indicator) tick(ts string) {
	if in.stopping() {
		return
	}
	if err := in.update(ts); err != nil {
		log.Printf("could not update the processing indicator: %s", logSafe(err.Error(), maxLoggedError))
	}
}

// setLabel attaches the status line the agent gave slack_progress, replacing
// whatever was there before, and asks the goroutine to show it now — which
// before the first post means posting now, grace period or not.
func (in *indicator) setLabel(label string) {
	in.labelMu.Lock()
	in.label = label
	in.labelMu.Unlock()

	select {
	case in.nudge <- struct{}{}:
	default:
	}
}

// notePosted records the message the indicator lives in, so a tool call can
// report it back without reaching into the goroutine.
func (in *indicator) notePosted(ts string) {
	in.labelMu.Lock()
	defer in.labelMu.Unlock()
	in.postedTS = ts
}

// messageTS is the indicator's message, or empty while it has yet to post one.
func (in *indicator) messageTS() string {
	in.labelMu.Lock()
	defer in.labelMu.Unlock()
	return in.postedTS
}

func (in *indicator) currentLabel() string {
	in.labelMu.Lock()
	defer in.labelMu.Unlock()
	return in.label
}

// finish closes done, which is the promise that the channel is clear of this
// indicator — and of the one it replaced, since a message can only be posted
// once its predecessor's is gone.
//
// That second half is why this is not a bare close. An indicator stopped
// before it ever posted still stands between its predecessor and its
// successor: closing done while the predecessor was mid-delete would let the
// next indicator post into a channel that still has the old message in it. The
// wait is bounded, because the predecessor's own work is.
func (in *indicator) finish() {
	if in.predecessor != nil {
		timeout := time.NewTimer(indicatorRequestTimeout + indicatorDeleteTimeout)
		defer timeout.Stop()
		select {
		case <-in.predecessor:
		case <-timeout.C:
		}
	}
	close(in.done)
}

// post publishes the indicator message.
//
// It runs on a detached context on purpose. A chat.postMessage abandoned in
// flight can still create the message, and the ts needed to delete it would be
// lost with the response — an indicator nobody can clear. Cancelling the
// session must not be able to orphan one, so the request is given its own
// deadline and allowed to finish; the deletion that follows is detached for
// the same reason.
//
// The cost is that the message can appear briefly after the agent has already
// answered, and is then deleted. Briefly late beats permanently stuck.
func (in *indicator) post() (string, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(in.ctx), indicatorRequestTimeout)
	defer cancel()
	return in.api.PostPlain(ctx, in.channel, in.threadTS, in.text())
}

// update is cancellable, unlike post: there is nothing to orphan, since the
// message it edits is already known and already scheduled for deletion.
func (in *indicator) update(ts string) error {
	ctx, cancel := context.WithTimeout(in.ctx, indicatorRequestTimeout)
	defer cancel()
	return in.api.Update(ctx, in.channel, ts, in.text())
}

// remove deletes the indicator message on a context of its own, so it still
// runs when the reason for stopping was the session shutting down.
func (in *indicator) remove(ts string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(in.ctx), indicatorDeleteTimeout)
	defer cancel()

	if err := in.api.Delete(ctx, in.channel, ts); err != nil {
		log.Printf("could not delete the processing indicator: %s", logSafe(err.Error(), maxLoggedError))
	}
}

func (in *indicator) text() string {
	elapsed := fmt.Sprintf("⏳ Working… (%s)", formatElapsed(time.Since(in.startedAt)))
	if label := in.currentLabel(); label != "" {
		return elapsed + " — " + label
	}
	return elapsed
}

// formatElapsed renders a duration the way a person reads a stopwatch: seconds
// while that is the interesting number, minutes and seconds once it has been a
// while, and hours and minutes past the hour, where a seconds count says
// nothing except that the agent is still going.
func formatElapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	total := int(d.Seconds())

	hours := total / 3600
	minutes := (total % 3600) / 60
	seconds := total % 60

	switch {
	case hours > 0:
		return fmt.Sprintf("%dh %02dm", hours, minutes)
	case minutes > 0:
		return fmt.Sprintf("%dm %02ds", minutes, seconds)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}
