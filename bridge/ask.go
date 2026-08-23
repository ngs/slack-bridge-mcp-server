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

// AskResult is what slack_ask returns. ChoiceIndex is -1 when no choice was
// made, so a timed-out answer cannot be misread as the first option.
//
// ChoiceLabel is the option as the caller wrote it, not as the button showed
// it: a label too long for Slack is shortened on the button, and handing back
// the shortened form would stop the caller from matching the answer against
// the list it passed in.
type AskResult struct {
	ChoiceIndex int    `json:"choice_index"`
	ChoiceLabel string `json:"choice_label,omitempty"`
	TS          string `json:"ts,omitempty"`
	TimedOut    bool   `json:"timed_out"`
}

// pendingAsk is the one question currently on the channel waiting for a click.
//
// ts and labels are guarded by the bridge's mutex; answered is a buffered
// channel so the routing side can hand the answer over without blocking, and
// without caring whether the asking goroutine is still there to take it.
type pendingAsk struct {
	ts     string
	labels []string
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
func (b *Bridge) Ask(ctx context.Context, question string, options []string, timeout time.Duration, threadTS string) (AskResult, error) {
	q, labels, err := buildQuestion(question, options)
	if err != nil {
		return AskResult{}, err
	}

	b.mu.Lock()
	if err := b.ensure(); err != nil {
		b.mu.Unlock()
		return AskResult{}, err
	}
	if b.ask != nil {
		b.mu.Unlock()
		return AskResult{}, errors.New("a question is already waiting for an answer; wait for it to be answered or to time out before asking another")
	}
	ask := &pendingAsk{labels: labels, answered: make(chan int, 1)}
	b.ask = ask
	api, channel, stream := b.api, b.cfg.Channel, b.stream
	b.mu.Unlock()

	defer b.clearAsk(ask)

	if api == nil {
		return AskResult{}, errors.New("the bridge is not connected to Slack")
	}

	// From here the agent is no longer the one working: the owner is. The
	// indicator's elapsed counter would be measuring their thinking time, so
	// retire it, and start a fresh one once they answer. Refusing the question
	// above leaves it alone, since nothing about the agent's work changed.
	retired := b.stopIndicator()

	ts, err := postQuestion(b.ctx, api, channel, threadTS, q)
	if err != nil {
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

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case choice := <-ask.answered:
			b.resolve(api, channel, ts, fmt.Sprintf("%s\n\n✅ %s", q.Text, labels[choice]))
			// The answer is new work handed to the agent, exactly like the
			// messages slack_wait returns, so the clock starts again here —
			// and in the thread the question was asked in, which is where the
			// owner just clicked and where they are watching for what follows.
			b.startIndicator(threadTS)
			return AskResult{ChoiceIndex: choice, ChoiceLabel: options[choice], TS: ts}, nil

		case <-deadline.C:
			// A click landing in the same instant as the deadline is still an
			// answer; the owner did decide, and honouring it costs nothing.
			if choice, ok := b.settleDeadline(ask, stream); ok {
				b.resolve(api, channel, ts, fmt.Sprintf("%s\n\n✅ %s", q.Text, labels[choice]))
				b.startIndicator(threadTS)
				return AskResult{ChoiceIndex: choice, ChoiceLabel: options[choice], TS: ts}, nil
			}
			b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
			return AskResult{ChoiceIndex: -1, TimedOut: true}, nil

		case <-ctx.Done():
			// The client gave up on the call. Nobody is left to receive an
			// answer, so the buttons have to go.
			b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
			return AskResult{}, ctx.Err()

		case <-b.ctx.Done():
			b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
			return AskResult{}, b.ctx.Err()

		case in, ok := <-stream.Interactions():
			if !ok {
				b.noteStreamClosed(stream)
				// The click channel closes with the socket, and a closed
				// channel is permanently ready — so this has to be handled
				// here or the loop spins on it. No answer can arrive now.
				b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
				return AskResult{}, errors.New("the Slack connection closed")
			}
			b.routeInteraction(in)

		case evt, ok := <-stream.Events():
			if !ok {
				b.noteStreamClosed(stream)
				// The socket is gone, so no click can reach this call any
				// more. The buttons have to go with it.
				b.resolve(api, channel, ts, q.Text+"\n\n⌛ expired")
				return AskResult{}, errors.New("the Slack connection closed")
			}
			// absorb routes the event: clicks reach this question, messages
			// queue up for the next slack_wait.
			if err := b.absorb(evt); err != nil {
				return AskResult{}, err
			}
		}
	}
}

// postQuestion posts on a detached context with a deadline of its own.
//
// The reason is the same as the indicator's: a chat.postMessage abandoned in
// flight can still create the message, and the ts needed to take the buttons
// away would be lost with the response — a question left hanging in the
// channel that no click can ever answer. The caller checks for cancellation
// once the ts is known, which is the point at which it can do something about
// it.
func postQuestion(ctx context.Context, api API, channel, threadTS string, q Question) (string, error) {
	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), askPostTimeout)
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

// drainInteractions takes everything already queued on the interaction channel
// and routes it, without waiting for more.
//
// The closed check is not a formality: a closed channel is permanently ready,
// so without it this loop would never reach its default and would spin for
// ever the moment the socket went away.
func (b *Bridge) drainInteractions(stream Stream) {
	for {
		select {
		case in, ok := <-stream.Interactions():
			if !ok {
				return
			}
			b.routeInteraction(in)
		default:
			return
		}
	}
}

// settleDeadline decides whether the question was answered after all.
//
// Three things can be true at the moment the timer fires: the answer is
// already waiting, a click is still sitting on the channel, or another
// goroutine has taken a click off the channel and is on its way to delivering
// it. select picks at random between a ready timer and a ready click, so the
// first two need looking at rather than assuming; the third is why this waits
// a moment and looks again, taking the mutex each time so that anything
// mid-delivery has landed by the time it does.
//
// A single dispatcher owning the click channel would remove the third case
// outright. It is not worth the machinery here: slack_wait already leaves the
// channel alone while a question is pending, so a competing reader only exists
// for the instant between the two.
func (b *Bridge) settleDeadline(ask *pendingAsk, stream Stream) (int, bool) {
	deadline := time.Now().Add(deadlineSettleWindow)
	for {
		b.drainInteractions(stream)
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

// clearAsk retires the question from the bridge, leaving a question that
// replaced it alone. Nothing can replace it while it is pending, so the guard
// is only there to keep a future change from clearing the wrong one.
func (b *Bridge) clearAsk(ask *pendingAsk) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ask == ask {
		b.ask = nil
	}
}

// resolve rewrites the question so it can no longer be clicked. It is best
// effort: a failure here leaves stale buttons in the channel, which is worth a
// log line but not worth failing an answer the agent already has.
func (b *Bridge) resolve(api API, channel, ts, text string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(b.ctx), askResolveTimeout)
	defer cancel()

	if err := api.ResolveQuestion(ctx, channel, ts, text); err != nil {
		log.Printf("could not retire the question in the channel: %s", logSafe(err.Error(), maxLoggedError))
	}
}

// routeInteraction takes b.mu and hands the click on. It exists so the two
// loops that read the stream do not have to know how a click is delivered.
func (b *Bridge) routeInteraction(in Interaction) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deliverInteraction(in)
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

	// Everything that can be checked without the timestamp is checked first,
	// so the buffer below only ever holds clicks that could plausibly be the
	// answer. Otherwise unrelated traffic during the posting window could fill
	// it and push the owner's real click out.
	if in.User != b.cfg.Owner || in.BlockID != askBlockID ||
		(in.Channel != "" && in.Channel != b.cfg.Channel) {
		ask.warn(in)
		return
	}

	if ask.ts == "" {
		// The question is posted but its timestamp has not come back yet.
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

	q := Question{
		BlockID: askBlockID,
		Text:    truncate(question, maxQuestionText),
		Options: make([]QuestionOption, 0, len(options)),
	}
	labels := make([]string, 0, len(options))

	for i, option := range options {
		label := truncate(strings.TrimSpace(option), maxOptionLabel)
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

// truncate shortens a string to limit characters, spending the last one on an
// ellipsis so the reader can tell something was cut.
func truncate(s string, limit int) string {
	if utf8.RuneCountInString(s) <= limit {
		return s
	}
	runes := []rune(s)
	return string(runes[:limit-1]) + "…"
}
