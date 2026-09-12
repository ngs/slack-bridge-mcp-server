package bridge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Bounds on a question. Slack allows far more buttons in one actions block,
// but ten is already more than anyone wants to pick from on a phone, and two
// is the point below which a question is really a statement.
const (
	MinAskOptions = 2
	MaxAskOptions = 10

	// maxOptionLabel is Slack's limit on button text. A longer label is
	// shortened rather than rejected: the model chose a wording that is too
	// long, which is not a reason to fail the owner's question.
	maxOptionLabel = 75
	// maxQuestionText is Slack's limit on a section block's text.
	maxQuestionText = 3000
)

// Identifiers the bridge puts on the question it posts, and recognises on the
// way back. The block id is stable across questions because only one question
// is ever outstanding.
const (
	askBlockID      = "slack_bridge_ask"
	askActionPrefix = "slack_bridge_choice_"
)

// askResolveTimeout bounds the chat.update that retires a question. Like the
// indicator's delete it runs on a detached context, since the usual reason for
// retiring a question early is that the call's context was just cancelled —
// and leaving live buttons behind is exactly what this call is preventing.
const askResolveTimeout = 5 * time.Second

// shutdownRetireWait is how long Close waits for a question's buttons to be
// taken out of the channel. It is the request's own bound and a moment more,
// so a retirement already in flight finishes rather than being cut off by the
// process exiting.
const shutdownRetireWait = askResolveTimeout + time.Second

// askPostTimeout bounds posting the question. It runs detached from the tool
// call, so this is what keeps a Slack that never answers from holding the call
// open past its own timeout.
const askPostTimeout = 10 * time.Second

// deadlineSettleWindow is how long the expiry waits for a click that another
// goroutine may be part-way through delivering. It is short enough that the
// timeout is still a timeout and long enough to cover a hand-off between two
// goroutines on the same machine.
const (
	deadlineSettleWindow = 250 * time.Millisecond
	deadlineSettleStep   = 10 * time.Millisecond
)

// maxEarlyClicks caps the clicks held while the question is being posted. The
// window is one HTTP round trip and only the owner's clicks can matter, so a
// handful is generous.
const maxEarlyClicks = 8

// AskRequest is the question to put to the owner, and where to put it.
type AskRequest struct {
	Question string
	Options  []string
	// Timeout is how long the owner has to answer, and it covers the whole
	// call: posting the question, waiting for a click, collecting whatever was
	// said instead of clicking, and taking the buttons away afterwards.
	//
	// The last of those is waited for only as long as there is. With nothing
	// left — a question that ran out of time, say — the request is still sent
	// and finishes on its own, and the call returns ahead of it rather than
	// over the timeout it was given.
	Timeout time.Duration
	// ThreadTS asks inside a thread instead of on the channel surface.
	ThreadTS string
	// Channel is the conversation to ask in. Empty means the home channel.
	Channel string
	// InterruptDisabled keeps the question waiting for a click even when the
	// owner says something instead of answering it.
	//
	// It is off by default — a message wins — because the owner typing rather
	// than tapping is nearly always them redirecting the agent, and the
	// alternative is the question blocking on a click that is never coming.
	// Turn it on for a question that genuinely has to be answered before
	// anything else can happen.
	InterruptDisabled bool
}

// AskResult is what slack_ask returns. ChoiceIndex is -1 when no choice was
// made, so a timed-out answer cannot be misread as the first option.
//
// ChoiceLabel is the option as the caller wrote it, not as the button showed
// it: a label too long for Slack is shortened on the button, and handing back
// the shortened form would stop the caller from matching the answer against
// the list it passed in.
// Interrupted is the third way a question can end, alongside an answer and a
// timeout: the owner said something instead of clicking. It is kept apart from
// TimedOut because the two call for opposite things — a timeout means nobody
// answered, an interruption means they answered with something the buttons
// could not express.
type AskResult struct {
	ChoiceIndex int    `json:"choice_index"`
	ChoiceLabel string `json:"choice_label,omitempty"`
	TS          string `json:"ts,omitempty"`
	TimedOut    bool   `json:"timed_out"`
	Interrupted bool   `json:"interrupted,omitempty"`
	// Messages are what the owner said while the question was on the channel.
	// A question blocks the loop that would otherwise have collected them, so
	// without this they would sit in the queue unseen until the next
	// slack_wait — which, if the answer sends the agent off to work, is a
	// while.
	//
	// Every settled outcome carries them, not just an interruption: a click and
	// an expiry leave the same backlog behind. Interrupted says only that a
	// message is why the question ended rather than something that happened
	// alongside it.
	//
	// They are delivered here exactly as slack_wait would have delivered them —
	// the same cursor moves, the same receipt reactions — so they are not owed
	// to a later call.
	Messages []Message `json:"messages,omitempty"`
}

// pendingAsk is the one question currently on the channel waiting for a click.
//
// ts and labels are guarded by the bridge's mutex; answered is a buffered
// channel so the routing side can hand the answer over without blocking, and
// without caring whether the asking goroutine is still there to take it.
type pendingAsk struct {
	ts string
	// generation is the connection the question was asked on. A click routed
	// by a later one answers a question that connection never saw.
	generation uint64
	// channel is where the question was posted, which is where a click has to
	// come from to be this question's answer.
	channel string
	labels  []string
	// answered carries the index of the option the owner clicked.
	answered chan int
	// warned keeps the log to one line per question when clicks that do not
	// belong to it keep arriving.
	warned bool
	// early holds clicks that arrived before the question had a timestamp to
	// match them against. They are replayed, and matched properly, as soon as
	// it does.
	early []Interaction
}

// Ask posts a multiple-choice question to the channel and blocks until the
// owner clicks an answer or the timeout expires.
//
// It is the bridge's counterpart to asking the user a question in the terminal:
// the agent stops, the owner decides, and the answer comes back as an index.
// The question stops being clickable either way — the answered case is
// rewritten with the choice, the expired case says so — so the owner never
// faces buttons that no longer lead anywhere.
func (b *Bridge) Ask(ctx context.Context, req AskRequest) (AskResult, error) {
	// A question is the agent listening too, so it counts towards presence for
	// as long as it is up — including the moments before the buttons exist and
	// after they are taken away.
	b.enterAsk()
	defer b.exitAsk()

	question, options := req.Question, req.Options
	threadTS, timeout := req.ThreadTS, req.Timeout
	// The clock the caller asked about starts here, before the question is
	// posted. Posting is a Slack request like any other and can take its time,
	// and a timeout measured from after it is a timeout the call can outlast
	// without ever having waited for an answer.
	callEnds := time.Now().Add(timeout)

	q, labels, err := buildQuestion(question, options)
	if err != nil {
		return AskResult{}, err
	}

	b.mu.Lock()
	if err := b.ensure(); err != nil {
		b.mu.Unlock()
		return AskResult{}, err
	}
	if b.ask != nil || b.askReserved {
		b.mu.Unlock()
		return AskResult{}, errors.New("a question is already waiting for an answer; wait for it to be answered or to time out before asking another")
	}
	channel := b.channelOr(req.Channel)
	ask := &pendingAsk{labels: labels, answered: make(chan int, 1), channel: channel, generation: b.connGeneration}
	// The reservation only. Refusing a second question is this call's from
	// here; being the thing a click answers is not, and will not be until
	// there is a question of its own in the channel.
	b.askReserved = true
	api, generation := b.api, b.connGeneration
	b.mu.Unlock()

	defer b.releaseAsk(ask)

	// A question from a call that was given up on mid-post may still be
	// standing in the channel with live buttons. Looked for here, before
	// another one goes up beside it: a click on the old one would answer
	// nothing.
	b.retireOrphanQuestion(ctx, api, time.Until(callEnds))

	// That search is a round trip, and the caller can have given up during it.
	// Asked before anything goes up, because a question posted for a call that
	// has already ended is buttons in the channel nobody is waiting on — put
	// there and taken away again in the same breath.
	if err := ctx.Err(); err != nil {
		return AskResult{}, err
	}

	if api == nil {
		return AskResult{}, errors.New("the bridge is not connected to Slack")
	}

	// From here the agent is no longer the one working: the owner is. The
	// indicator's elapsed counter would be measuring their thinking time, so
	// retire it, and start a fresh one once they answer. Refusing the question
	// above leaves it alone, since nothing about the agent's work changed.
	retired := b.stopIndicator()

	budget := time.Until(callEnds)
	if budget <= 0 {
		// The time the caller asked for went on getting here — the search for
		// an abandoned question, most likely. Posting one now would put
		// buttons in the channel for a call that is already over.
		b.restoreIndicator(ctx, retired)
		return AskResult{ChoiceIndex: -1, TimedOut: true}, nil
	}

	// Published here, and not when the call was accepted. From this moment a
	// click on this block is plausibly this question's answer and is worth
	// holding; before it, the only question in the channel with these buttons
	// is an older one the search above was trying to retire, and holding a
	// click on that would replay it against the message that replaces it.
	b.publishAsk(ask)

	// Taken before the post, because it is the window the search for an
	// abandoned one is cut by: anything the bridge posted after this moment.
	attempted := time.Now()
	ts, err := postQuestion(b.ctx, api, channel, threadTS, q, budget)
	if err != nil {
		// A post given up on can still have landed: the request was
		// abandoned, not cancelled at Slack. What is lost with it is the ts,
		// which is the only thing that could take the buttons away — so the
		// question is remembered instead, and the next one looks for it.
		if errors.Is(err, context.DeadlineExceeded) {
			b.noteOrphanQuestion(channel, threadTS, attempted)
		}
		// No question went up, so the agent is still the one working and the
		// channel should say so again.
		b.restoreIndicator(ctx, retired)
		return AskResult{}, err
	}

	// The question is live from here, and clicks that arrived while it was
	// being posted have been waiting for this.
	b.adoptQuestion(ask, ts)

	// The post deliberately outlives a cancelled call, so the cancellation has
	// to be answered here instead — with the ts in hand, the buttons can
	// actually be taken away.
	if err := ctx.Err(); err != nil {
		b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
		return AskResult{}, err
	}

	// Subscribed even when interruption is off, because the subscription is
	// also how this call learns about a message it absorbed itself; the flag
	// only decides what it does about it.
	sub := b.subscribePending()
	defer b.unsubscribePending(sub)

	// What is left of it, rather than the whole of it again: posting has
	// already spent some, and the caller was promised one timeout and not two.
	askEnds := callEnds
	deadline := time.NewTimer(time.Until(askEnds))
	defer deadline.Stop()

	// The conversations a walk could not reach are worth one look, not one per
	// wakeup: what answers them is a round trip, and one that stays out of
	// budget would buy another every time this question woke.
	skippedLooked := false

	for {
		// Both of these are checked before blocking, not only on a wakeup. The
		// pump applies what the socket delivers as it arrives, and it can have
		// done so — and sent its notification — while this question was still
		// being posted, which is before there was anything subscribed to hear
		// it.
		//
		// The caller giving up, or the session ending, comes first: both of
		// those end the pump as well, and answering "the connection closed" to
		// a cancelled call describes the consequence rather than the cause.
		if err := callerGone(ctx, b.ctx); err != nil {
			b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
			return AskResult{}, err
		}
		// No click can reach this question once the socket is gone, so the
		// buttons go with it rather than standing there inviting an answer
		// nothing could carry.
		if b.streamGone(generation) {
			// The connection hands over what it had before it says it is
			// gone, so the owner's click can already be waiting here. It is an
			// answer whatever happened to the socket afterwards.
			if choice, ok := b.lastChance(ask); ok {
				return b.answered(ctx, api, answer{
					channel: channel, threadTS: threadTS, ts: ts, generation: generation, ends: askEnds,
					q: q, labels: labels, options: options, choice: choice,
				}), nil
			}
			b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
			return AskResult{}, errors.New("the Slack connection closed")
		}
		if !req.InterruptDisabled && b.backlogWaiting(!skippedLooked) {
			skippedLooked = true
			if msgs := b.backlogWhileAsking(ctx, generation, time.Until(askEnds)); len(msgs) > 0 {
				return b.interrupted(api, channel, ts, q, msgs, askEnds), nil
			}
			// A reconnect rather than a message, or a drain that failed and
			// left the queue where it was. Either way there is nothing to act
			// on, so the question stands and this waits like any other.
		}

		select {
		case <-sub:
			if req.InterruptDisabled {
				continue
			}
			msgs := b.backlogWhileAsking(ctx, generation, time.Until(askEnds))
			if len(msgs) == 0 {
				continue
			}
			return b.interrupted(api, channel, ts, q, msgs, askEnds), nil

		case choice := <-ask.answered:
			return b.answered(ctx, api, answer{
				channel: channel, threadTS: threadTS, ts: ts, generation: generation, ends: askEnds,
				q: q, labels: labels, options: options, choice: choice,
			}), nil

		case <-deadline.C:
			// A click landing in the same instant as the deadline is still an
			// answer; the owner did decide, and honouring it costs nothing.
			if choice, ok := b.settleDeadline(ask); ok {
				return b.answered(ctx, api, answer{
					channel: channel, threadTS: threadTS, ts: ts, generation: generation, ends: askEnds,
					q: q, labels: labels, options: options, choice: choice,
				}), nil
			}
			// Within what is left, which by now is the floor: the call is
			// over either way, and the promise about when it returns outlives
			// the question it was about.
			b.resolveWithin(api, channel, ts, q.Text+"\n\n⌛ expired", time.Until(askEnds))
			// The deadline and the end of the connection can be ready at the
			// same moment, and a timeout would be the wrong answer to the
			// second: it says "ask again", which is not what a call does with
			// a socket that has gone. A cancellation outranks both.
			if err := callerGone(ctx, b.ctx); err != nil {
				return AskResult{}, err
			}
			if b.streamGone(generation) {
				return AskResult{}, errors.New("the Slack connection closed")
			}
			// A question nobody answered leaves the agent with nothing to act
			// on, which is exactly when a message waiting behind it matters
			// most.
			return AskResult{ChoiceIndex: -1, TimedOut: true, Messages: b.backlogWhileAsking(ctx, generation, askLastLookWait)}, nil

		case <-ctx.Done():
			// The client gave up on the call. Nobody is left to receive an
			// answer, so the buttons have to go.
			b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
			return AskResult{}, ctx.Err()

		case <-b.ctx.Done():
			b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
			return AskResult{}, b.ctx.Err()

		}
	}
}

// callerGone reports the cancellation, if either context has one. The call's
// own comes first: it is the more specific answer to "why did this stop".
func callerGone(call, session context.Context) error {
	if err := call.Err(); err != nil {
		return err
	}
	return session.Err()
}

// backlogSlotWait is how long collecting the backlog will wait for its turn at
// catch-up. A settled question is answering the owner, so it waits a while, but
// not on a request that has clearly gone wrong.
const backlogSlotWait = 5 * time.Second

// askLastLookWait is what a backlog drain gets once the question is over: its
// deadline has passed, so this is a courtesy rather than a budget — long enough
// for a request that is nearly done, short enough that the answer is prompt.
const askLastLookWait = 250 * time.Millisecond

// backlogWaiting reports whether there is anything for a question to be
// interrupted by: messages the pump has queued, or a catch-up that has not run
// and may find some.
//
// withSkipped adds the conversations a walk ran out of budget for. Their
// replies are in neither queue yet and only a walk will bring them, so a
// question that ignored them would be asked over the top of something the
// owner said. It is asked at most once per question: the walk that answers it
// is a round trip, and a conversation that stays out of budget would otherwise
// buy one on every wakeup.
func (b *Bridge) backlogWaiting(withSkipped bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pending) > 0 || len(b.pendingThreads) > 0 || b.needCatchUp {
		return true
	}
	return withSkipped && b.threadsSkipped
}

// answer is everything the settled question needs to report itself, gathered so
// the three places a click can arrive say it the same way.
type answer struct {
	channel  string
	threadTS string
	ts       string
	// ends is when the question's own timeout runs out, kept as the moment
	// rather than as what was left of it at some earlier point. Retiring the
	// buttons and collecting the backlog both come out of it, one after the
	// other, and a remainder measured once and used twice would let the second
	// of them start after the deadline had already passed.
	ends time.Time
	// generation is the connection the question was asked on, which is what
	// decides whether the backlog it collects is still its to collect.
	generation uint64
	q          Question
	labels     []string
	options    []string
	choice     int
}

// answered retires a question the owner clicked, and reports the choice.
//
// The answer is new work handed to the agent, exactly like the messages
// slack_wait returns, so the clock starts again here — and in the thread the
// question was asked in, which is where the owner just clicked and where they
// are watching for what follows.
func (b *Bridge) answered(ctx context.Context, api API, a answer) AskResult {
	b.resolveWithin(api, a.channel, a.ts, answeredText(a.q.Text, a.labels[a.choice]), time.Until(a.ends))
	b.startIndicator(a.channel, a.threadTS)
	return AskResult{
		ChoiceIndex: a.choice,
		ChoiceLabel: a.options[a.choice],
		TS:          a.ts,
		// Measured again, not reused: retiring the buttons has just spent some
		// of what was left, and the caller was promised one timeout.
		Messages: b.backlogWhileAsking(ctx, a.generation, time.Until(a.ends)),
	}
}

// interrupted retires a question the owner answered with words instead of a
// button, and reports what they said.
//
// The buttons go: leaving them up invites a click on a question that is no
// longer the thing being answered. The clock restarts where the owner sent the
// message rather than where the question was asked, because that message is the
// new work.
func (b *Bridge) interrupted(api API, channel, ts string, q Question, msgs []Message, ends time.Time) AskResult {
	// Inside the question's own deadline like every other settled outcome. The
	// caller is waiting on this one — it has the owner's message in hand — so
	// the retirement is dispatched and waited for only as long as there is.
	b.resolveWithin(api, channel, ts, q.Text+"\n\n⌛ superseded", time.Until(ends))
	b.startIndicator(newestConversation(msgs))
	return AskResult{
		ChoiceIndex: -1,
		Interrupted: true,
		TS:          ts,
		Messages:    msgs,
	}
}

// backlogWhileAsking hands over whatever the owner said while the question was
// waiting, exactly the way slack_wait would have: the same drain, so the cursor
// moves and is persisted, and the same receipt reactions.
//
// A question is the one time the agent is listening and deliberately not
// collecting. Messages that arrive meanwhile are absorbed into the pending
// queue, and until this they stayed there — the owner asked something, got a
// question back, answered it, and their message went unanswered while the agent
// acted on the click. Returning them with the answer closes that window.
//
// It is best effort on purpose. A failed drain leaves the bridge ready to fetch
// the same window again — drainCatchUp only moves the cursor once the messages
// are in hand — so the next slack_wait finds them, and the answer the agent
// already has must not be lost to a history call that failed.
//
// It is called only once a question has settled. A call abandoned or a socket
// that closed has no session left to hand a backlog to, and moving the cursor
// there would consume messages nobody ever received.
func (b *Bridge) backlogWhileAsking(ctx context.Context, generation uint64, budget time.Duration) []Message {
	// Bounded by what the question has left. The slot wait was already, but
	// the request itself was not: a history call that hangs would hold the
	// tool past the timeout the caller asked for, which is the one promise
	// every tool makes. Nothing is committed until the messages are in hand,
	// so a drain cut short here costs a round trip and no messages.
	//
	// A budget already spent buys nothing at all, and a small one buys only
	// what it is: the courtesy of a quarter of a second belongs to the call
	// that has run out of time and has nothing else to return, and it is that
	// call which asks for it by name. A call holding an answer has something
	// better to do with the moment, and the next slack_wait reads the same
	// window.
	if budget <= 0 {
		return nil
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, budget)
	defer cancelDrain()

	slotWait := backlogSlotWait
	if budget < slotWait {
		slotWait = budget
	}

	msgs, _, err := b.drainCatchUp(drainCtx, generation, false, slotWait)
	if err != nil {
		if ctx.Err() == nil && drainCtx.Err() != nil {
			// The question's own deadline, not the caller's. The next
			// slack_wait reads the same window.
			return nil
		}
		log.Printf("could not collect the messages that arrived while the question was pending: %s", logSafe(err.Error(), maxLoggedError))
		return nil
	}
	b.autoAck(msgs)
	return msgs
}

// orphanQuestion is a question whose post was abandoned before its timestamp
// came back. It may be standing in the channel with live buttons, and the
// timestamp that would retire it went with the request.
type orphanQuestion struct {
	channel string
	// threadTS is set for a question asked inside a conversation, which is
	// where it has to be looked for: a thread reply is in no channel history.
	threadTS string
	// since and until are the window the question can be in, as Slack
	// timestamps. The first is the attempt, less a margin: the clock here is
	// not Slack's, and both ends of a history window are exclusive. The second
	// is as long after it as the post could possibly have taken, which keeps
	// the page from filling with a busy channel's later traffic.
	since string
	until string
	// attempts is how many calls have tried to retire it. A question that
	// cannot be reached is not worth every later question's budget.
	attempts int
}

// noteOrphanQuestion remembers a post that was given up on.
func (b *Bridge) noteOrphanQuestion(channel, threadTS string, attempted time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.orphan = &orphanQuestion{
		channel:  channel,
		threadTS: threadTS,
		since:    slackTS(attempted.Add(-orphanClockSlack)),
		until:    slackTS(attempted.Add(askPostTimeout + orphanClockSlack)),
	}
}

// slackTS renders a moment the way Slack timestamps one, which is what the
// history and replies windows are cut by.
func slackTS(at time.Time) string {
	return fmt.Sprintf("%d.%06d", at.Unix(), at.Nanosecond()/1000)
}

// retireOrphanQuestion takes away the buttons of a question whose post was
// abandoned, if it finds one. Best effort by nature: the question may never
// have landed, and looking costs one history call — so it is looked for once,
// on the next question, and forgotten either way.
func (b *Bridge) retireOrphanQuestion(ctx context.Context, api API, budget time.Duration) {
	b.mu.Lock()
	orphan := b.orphan
	b.orphan = nil
	me := b.botUserID
	b.mu.Unlock()

	if orphan == nil {
		return
	}
	if api == nil {
		// Nothing was looked at, so nothing was learned. Put back for a call
		// that has a connection to look with.
		b.keepOrphanQuestion(orphan)
		return
	}
	if me == "" {
		// Without a user ID of its own the bridge cannot tell its message from
		// anybody else's, and a search that guessed would retire somebody's.
		// Said plainly: the question it was looking for keeps its buttons, and
		// a click on them answers nothing.
		log.Printf("cannot look for a question whose post was given up on: this app's own user ID is not known, so its buttons stay until somebody takes them away")
		return
	}
	// Inside the question's own budget, like everything else the call does:
	// looking for a question that may never have landed is not worth the
	// timeout the caller was promised.
	if budget <= 0 {
		// Not looked at either, so the next question with time to spare is the
		// one to look. No attempt counted: nothing was spent on this.
		b.keepOrphanQuestion(orphan)
		return
	}
	if budget > orphanSearchWait {
		budget = orphanSearchWait
	}
	ends := time.Now().Add(budget)
	searchCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	found, err := b.findOrphanQuestion(searchCtx, api, orphan, me)
	if err != nil {
		// A search that ran out of time, or a caller that gave up during it,
		// has learned nothing about the question — so it goes back on the
		// shelf for the next one, the same way a retirement that could not be
		// finished does. Anything else is Slack saying no, which the next
		// question is not going to talk it out of.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			orphan.attempts++
			if orphan.attempts <= maxOrphanAttempts {
				b.keepOrphanQuestion(orphan)
				log.Printf("did not finish looking for a question whose post was given up on; the next question will look again: %s", logSafe(err.Error(), maxLoggedError))
				return
			}
			// Tried often enough. A question that cannot be reached is not
			// worth every later question's budget.
		}
		log.Printf("could not look for a question whose post was given up on: %s", logSafe(err.Error(), maxLoggedError))
		return
	}
	if found == "" {
		// Either it never landed, or it is outside the window, or somebody has
		// taken it away already. Said plainly, because the one case that
		// matters — a question standing in the channel that nothing will ever
		// retire — looks exactly like the other two from here.
		log.Printf("found no question to retire from a post that was given up on; if one did land, its buttons are still there")
		return
	}
	// What is left of the budget, not what it was: the search has just spent
	// some of it, and the two of them are one budget between them.
	//
	// No floor here, unlike a question this call owns. If there is not enough
	// left, or Slack will not take it, the question goes back on the shelf and
	// the next one tries again — which is the difference between a message
	// somebody else will go back for and one nobody will.
	//
	// Remembered as retired before the request goes out, the same way a
	// question this call owns is. Whether or not Slack takes it, those buttons
	// are not the next question's answer — and this call is about to post that
	// question, in the window where a tap on the old one cannot be told apart
	// by timestamp.
	b.mu.Lock()
	b.retiredTS = found
	b.mu.Unlock()

	if err := b.tryResolve(api, orphan.channel, found, orphanExpiredText, time.Until(ends)); err != nil {
		orphan.attempts++
		if !errors.Is(err, context.DeadlineExceeded) || orphan.attempts > maxOrphanAttempts {
			// Slack refused it rather than running out of time, or it has been
			// tried often enough. Either way the next question is not the one
			// to keep paying for this: said plainly and let go of.
			log.Printf("could not retire a question whose post was given up on, and will not try again: %s", logSafe(err.Error(), maxLoggedError))
			return
		}
		log.Printf("ran out of time retiring a question whose post was given up on; the next question will try again: %s", logSafe(err.Error(), maxLoggedError))
		b.keepOrphanQuestion(orphan)
	}
}

// keepOrphanQuestion puts an abandoned question back for the next call to try,
// leaving a newer one alone: this call's own post may have been given up on
// too while it was busy with this, and that one is the more recent loss.
func (b *Bridge) keepOrphanQuestion(orphan *orphanQuestion) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.orphan == nil {
		b.orphan = orphan
	}
}

// findOrphanQuestion looks for the message an abandoned post may have left, and
// reports its timestamp.
//
// What identifies it is the block the bridge puts its buttons in, inside a
// window around the attempt. Not the text: Slack rewrites what it stores,
// turning & into &amp; and a bare link into <link>, so a comparison that looked
// exact would quietly find nothing at all. And not the newest thing this app
// posted either — after a post that failed it posts others, the indicator
// saying it is working and the reply to whatever prompted the question — and
// expiring one of those rewrites a message the owner is reading.
//
// A question that has already been retired has no such block: retiring one
// replaces the block list. So the one thing this looks for says both that the
// message is the bridge's own question and that its buttons are still live.
func (b *Bridge) findOrphanQuestion(ctx context.Context, api API, orphan *orphanQuestion, me string) (string, error) {
	var messages []candidate
	if orphan.threadTS != "" {
		// A reply is in no channel history, so the conversation is where it
		// has to be looked for.
		replies, err := api.Replies(ctx, RepliesRequest{
			Channel:  orphan.channel,
			ThreadTS: orphan.threadTS,
			Oldest:   orphan.since,
			Latest:   orphan.until,
			Limit:    orphanSearchLimit,
		})
		if err != nil {
			return "", err
		}
		messages = replies.Messages
	} else {
		// Both ends of the window. History comes back newest first and the
		// limit counts from that end, so a channel that has been busy since
		// would otherwise fill the page with what came after and leave the
		// question outside it.
		page, err := api.History(ctx, HistoryRequest{
			Channel: orphan.channel,
			Oldest:  orphan.since,
			Latest:  orphan.until,
			Limit:   orphanSearchLimit,
		})
		if err != nil {
			return "", err
		}
		messages = page.Messages
	}

	found := ""
	for _, c := range messages {
		// The bridge's own question, with its buttons still on it. The block
		// id says both: nobody else posts it, and retiring a question takes
		// the block away. Without this the newest thing the bridge posted in
		// the window would do — and after a post that failed, that is the
		// indicator saying "Working…", or the reply to the message that
		// prompted the question.
		if c.User != me || c.TS == "" || !c.HasAskButtons {
			continue
		}
		// The newest of them: history comes back newest first and replies
		// oldest first, so the comparison rather than the order decides.
		if tsLess(found, c.TS) {
			found = c.TS
		}
	}
	return found, nil
}

// orphanSearchLimit is how many messages the search for an abandoned question
// reads. The window it reads is only as wide as a post can take, so this is
// generous for it — and it is one request either way, which is what the budget
// is being spent on.
const orphanSearchLimit = 100

// orphanExpiredText is what an abandoned question is retired with. Its own
// text is not repeated: what Slack stored may not be what was sent.
const orphanExpiredText = "⌛ expired"

// maxOrphanAttempts is how many questions will pay for retiring an earlier
// one that could not be reached.
const maxOrphanAttempts = 3

// orphanClockSlack is how far either side of the attempt the window reaches.
// The moment is taken from this machine's clock and the timestamps come from
// Slack's, and both ends of a window are exclusive — so a question posted in
// the same second could sit just outside a window cut to the instant. Being
// generous costs nothing now that the block id says which message is the
// question.
const orphanClockSlack = 5 * time.Second

// orphanSearchWait bounds the search itself. It is one request, and the call
// that pays for it is about to ask a question of its own.
const orphanSearchWait = 2 * time.Second

// postQuestion posts on a detached context with a deadline of its own.
//
// The reason is the same as the indicator's: a chat.postMessage abandoned in
// flight can still create the message, and the ts needed to take the buttons
// away would be lost with the response — a question left hanging in the
// channel that no click can ever answer. The caller checks for cancellation
// once the ts is known, which is the point at which it can do something about
// it.
func postQuestion(ctx context.Context, api API, channel, threadTS string, q Question, budget time.Duration) (string, error) {
	// The smaller of the two: a post that outlasts the question's own timeout
	// has already broken the promise the timeout is, and one that outlasts the
	// bound below has stopped being worth waiting for either way. A caller
	// with nothing left does not get here at all.
	if budget > askPostTimeout {
		budget = askPostTimeout
	}

	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	return api.PostQuestion(postCtx, channel, threadTS, q)
}

// adoptQuestion gives the pending ask its timestamp and replays the clicks
// that arrived before it had one.
//
// That window is real: Slack shows the buttons the moment the message is
// created, which is before chat.postMessage answers, and a concurrent
// slack_wait is reading the same interaction channel. Without the replay, an
// owner quick on the draw would tap an answer that went nowhere.
func (b *Bridge) adoptQuestion(ask *pendingAsk, ts string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ask.ts = ts
	early := ask.early
	ask.early = nil
	for _, in := range early {
		b.deliverInteraction(in)
	}
}

// settleDeadline decides whether the question was answered after all.
//
// The pump routes clicks as it receives them, so a click that arrived in the
// same instant as the deadline is already on its way to this question rather
// than sitting on a channel waiting to be read. What is left is the moment
// between the routing and the answer being taken, which is why this looks more
// than once: the owner did decide, and honouring it costs nothing.
func (b *Bridge) settleDeadline(ask *pendingAsk) (int, bool) {
	deadline := time.Now().Add(deadlineSettleWindow)
	for {
		if choice, ok := b.lastChance(ask); ok {
			return choice, true
		}
		if time.Now().After(deadline) {
			return 0, false
		}
		time.Sleep(deadlineSettleStep)
	}
}

// lastChance reports an answer already queued but not yet taken, which is the
// deadline racing a click.
func (b *Bridge) lastChance(ask *pendingAsk) (int, bool) {
	select {
	case choice := <-ask.answered:
		return choice, true
	default:
		return 0, false
	}
}

// publishAsk makes the question the one a click answers. Called once the
// buttons are about to exist, which is the first moment a click on this block
// could belong to it.
func (b *Bridge) publishAsk(ask *pendingAsk) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ask = ask
}

// releaseAsk gives up both the reservation and the question itself, which is
// the end of the call either way — whether it ever got as far as posting.
//
// The guard on the question is only there to keep a future change from
// clearing the wrong one: nothing can replace it while the reservation is
// held, and the reservation outlives it.
func (b *Bridge) releaseAsk(ask *pendingAsk) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ask == ask {
		b.ask = nil
	}
	b.askReserved = false
}

// resolve rewrites the question so it can no longer be clicked. It is best
// effort: a failure here leaves stale buttons in the channel, which is worth a
// log line but not worth failing an answer the agent already has.
func (b *Bridge) resolve(api API, channel, ts, text string) {
	b.resolveWithin(api, channel, ts, text, askResolveTimeout)
}

// resolveWithin is resolve with a bound on how long the caller waits for it.
//
// The request itself always gets the whole of askResolveTimeout, on a detached
// context and on a goroutine of its own. What budget bounds is the waiting,
// not the retiring.
//
// Splitting the two is what lets both promises stand at once. The buttons have
// to go — a question this call owns is one nothing else will ever go back for,
// so a request abandoned because the timeout ran out would leave them standing
// for good. And the timeout covers the whole call, so waiting the request out
// past the deadline is the one promise every tool makes, broken at the last
// step. Neither is necessary once the request outlives the wait: the caller
// returns on time and the channel is tidied behind it.
//
// It is still waited for when there is time, because that is the ordinary
// case: Slack answers in well under the budget, and the owner sees the answer
// on the question rather than a moment after it.
func (b *Bridge) resolveWithin(api API, channel, ts, text string, budget time.Duration) {
	done := b.retireQuestion(api, channel, ts, text)
	if budget <= 0 {
		return
	}

	timeout := time.NewTimer(budget)
	defer timeout.Stop()
	select {
	case <-done:
	case <-timeout.C:
		// Still in flight, and it will finish on its own. The caller has what
		// it was waiting for and a deadline it was given.
	}
}

// retireQuestion sends the chat.update that takes a question's buttons away,
// and reports when it is over.
//
// On a goroutine because it deliberately outlives the call: see resolveWithin.
// Counted so Close can wait for it, and not counted once Close has started
// waiting — a count added behind the wait is the one way a WaitGroup can be
// misused, and a retirement started that late is one the process is not going
// to wait for anyway.
func (b *Bridge) retireQuestion(api API, channel, ts, text string) <-chan struct{} {
	done := make(chan struct{})

	b.mu.Lock()
	// Remembered before the request goes out, so a tap on these buttons counts
	// as stale from the moment the bridge decided they were.
	b.retiredTS = ts
	counted := !b.retireSealed
	if counted {
		b.retiring.Add(1)
	}
	b.mu.Unlock()

	go func() {
		defer close(done)
		if counted {
			defer b.retiring.Done()
		}
		if err := b.tryResolve(api, channel, ts, text, askResolveTimeout); err != nil {
			log.Printf("could not retire the question in the channel: %s", logSafe(err.Error(), maxLoggedError))
		}
	}()
	return done
}

// awaitRetirements waits for the question retirements still in flight, so a
// process on its way out does not leave live buttons behind for the sake of
// the moment they had left to run.
func (b *Bridge) awaitRetirements() {
	done := make(chan struct{})
	go func() {
		b.retiring.Wait()
		close(done)
	}()

	timeout := time.NewTimer(shutdownRetireWait)
	defer timeout.Stop()
	select {
	case <-done:
	case <-timeout.C:
		log.Printf("gave up waiting for a question's buttons to be taken out of the channel")
	}
}

// tryResolve is the request itself, bounded and reported. The caller decides
// what a failure means.
func (b *Bridge) tryResolve(api API, channel, ts, text string, budget time.Duration) error {
	if budget > askResolveTimeout {
		budget = askResolveTimeout
	}
	if budget <= 0 {
		return context.DeadlineExceeded
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(b.ctx), budget)
	defer cancel()

	return api.ResolveQuestion(ctx, channel, ts, text)
}

// deliverInteraction hands a button click to the pending question, if it is
// one of its buttons and the owner is the one who clicked. The caller must
// hold b.mu.
//
// Everything else is dropped. The click is already acknowledged to Slack by
// the socket layer, so dropping it here only means the bridge has no use for
// it — never that Slack will retry.
func (b *Bridge) deliverInteraction(in Interaction) {
	ask := b.ask
	if ask == nil {
		return
	}
	if ask.generation != b.connGeneration {
		// The question was asked on a connection that has since been replaced,
		// and is about to be told so. A click routed by the replacement is not
		// its answer: it belongs to whatever is asked next.
		return
	}

	// Everything that can be checked without the timestamp is checked first,
	// so the buffer below only ever holds clicks that could plausibly be the
	// answer. Otherwise unrelated traffic during the posting window could fill
	// it and push the owner's real click out.
	if in.User != b.cfg.Owner || in.BlockID != askBlockID ||
		(in.Channel != "" && in.Channel != ask.channel) {
		ask.warn(in)
		return
	}

	if ask.ts == "" {
		// The question is posted but its timestamp has not come back yet.
		//
		// One stale click can be recognised even here: the question whose
		// buttons this session has just sent away. That request outlives the
		// call that made it, so the old buttons can still be tappable while
		// this question is going up, and taps on them would otherwise fill the
		// buffer below and push out the click the owner actually meant.
		//
		// Only in this window. Once the question has a timestamp of its own,
		// that timestamp is the authority and the comparison below is the one
		// that decides.
		if in.MessageTS != "" && in.MessageTS == b.retiredTS {
			ask.warn(in)
			return
		}
		// Holding the click keeps it out of the bin until it can be checked
		// against the message it belongs to; the cap is there because a click
		// that never matches must not accumulate.
		if len(ask.early) < maxEarlyClicks {
			ask.early = append(ask.early, in)
		}
		return
	}

	// A ts identifies a message only within its channel, and the question is
	// the only message this call may be answered from.
	if in.MessageTS != ask.ts {
		ask.warn(in)
		return
	}

	choice, err := choiceIndex(in.ActionID, in.Value)
	if err != nil || choice < 0 || choice >= len(ask.labels) {
		log.Printf("ignoring a button click with an unrecognised action %s", logSafe(in.ActionID, maxLoggedValue))
		return
	}

	select {
	case ask.answered <- choice:
	default:
		// Already answered. The second click is the owner tapping twice
		// before Slack removed the buttons, and the first answer stands.
	}
}

// warn reports a click that is not this question's answer, once per question:
// a stale message with live buttons can be tapped repeatedly, and each tap is
// otherwise a log line.
func (a *pendingAsk) warn(in Interaction) {
	if a.warned {
		return
	}
	a.warned = true
	log.Printf("ignoring a button click that is not the owner answering the pending question (user %s, channel %s, message %s)",
		logSafe(in.User, maxLoggedValue), logSafe(in.Channel, maxLoggedValue), logSafe(in.MessageTS, maxLoggedValue))
}

// choiceIndex reads the option index back out of a click. The value is the
// authority and the action id is the fallback, since both carry it and either
// one alone is enough to identify the button.
func choiceIndex(actionID, value string) (int, error) {
	if value != "" {
		return strconv.Atoi(value)
	}
	return strconv.Atoi(strings.TrimPrefix(actionID, askActionPrefix))
}

// buildQuestion validates the tool arguments and turns them into the message
// to post, returning the labels as the owner will see them.
func buildQuestion(question string, options []string) (Question, []string, error) {
	if strings.TrimSpace(question) == "" {
		return Question{}, nil, errors.New("question is required")
	}
	if len(options) < MinAskOptions || len(options) > MaxAskOptions {
		return Question{}, nil, fmt.Errorf("options must hold between %d and %d choices, got %d", MinAskOptions, MaxAskOptions, len(options))
	}

	// Option labels stay exactly as written: they render as plain text on a
	// button, where Markdown is not markup but literal asterisks.
	q := Question{
		BlockID: askBlockID,
		Text:    truncate(question, maxQuestionText),
		Options: make([]QuestionOption, 0, len(options)),
	}
	labels := make([]string, 0, len(options))

	for i, option := range options {
		// Flattened, not merely trimmed at the ends: a line break inside a
		// label survives on the button, where plain_text keeps it, and is
		// turned into a space wherever the label is quoted back as Markdown.
		// One shape for both is worth more than the break.
		label := truncate(flattenLines(strings.TrimSpace(option)), maxOptionLabel)
		if label == "" {
			return Question{}, nil, fmt.Errorf("option %d is empty; every choice needs a label", i)
		}
		index := strconv.Itoa(i)
		q.Options = append(q.Options, QuestionOption{
			ActionID: askActionPrefix + index,
			Value:    index,
			Label:    label,
		})
		labels = append(labels, label)
	}
	return q, labels, nil
}

// flattenLines replaces each run of whitespace that contains a line break with
// a single space, and leaves every other run exactly as it was.
//
// Only the breaks: a label written with two spaces or a tab in it keeps them,
// because plain_text would have shown them and nothing about quoting the
// label later makes them a problem.
func flattenLines(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	for i := 0; i < len(s); {
		if !isSpace(s[i]) {
			b.WriteByte(s[i])
			i++
			continue
		}

		run := i
		for i < len(s) && isSpace(s[i]) {
			i++
		}
		if strings.ContainsAny(s[run:i], "\n\r") {
			b.WriteByte(' ')
			continue
		}
		b.WriteString(s[run:i])
	}
	return b.String()
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// answeredText is the question with the chosen answer under it.
//
// The label is quoted literally because it was chosen on a button, where
// plain_text showed it exactly as the caller wrote it. The resolved question
// is Markdown, where an option like **yes** would arrive in bold, <https://x>
// as a link and :shipit: as a picture — none of them what the owner clicked.
func answeredText(question, label string) string {
	return fmt.Sprintf("%s\n\n✅ %s", question, literal(label))
}

// truncate shortens a string to limit characters, spending the last one on an
// ellipsis so the reader can tell something was cut.
func truncate(s string, limit int) string {
	if utf8.RuneCountInString(s) <= limit {
		return s
	}
	runes := []rune(s)
	return string(runes[:limit-1]) + "…"
}
