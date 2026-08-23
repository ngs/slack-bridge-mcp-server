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
	b.stopIndicator()

	ts, err := api.PostQuestion(ctx, channel, threadTS, q)
	if err != nil {
		return AskResult{}, err
	}

	b.mu.Lock()
	ask.ts = ts
	b.mu.Unlock()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case choice := <-ask.answered:
			b.resolve(api, channel, ts, fmt.Sprintf("%s\n\n✅ %s", q.Text, labels[choice]))
			// The answer is new work handed to the agent, exactly like the
			// messages slack_wait returns, so the clock starts again here.
			b.startIndicator()
			return AskResult{ChoiceIndex: choice, ChoiceLabel: options[choice], TS: ts}, nil

		case <-deadline.C:
			// A click landing in the same instant as the deadline is still an
			// answer; the owner did decide, and honouring it costs nothing.
			if choice, ok := b.lastChance(ask); ok {
				b.resolve(api, channel, ts, fmt.Sprintf("%s\n\n✅ %s", q.Text, labels[choice]))
				b.startIndicator()
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

		case in := <-stream.Interactions():
			b.routeInteraction(in)

		case evt, ok := <-stream.Events():
			if !ok {
				b.mu.Lock()
				b.connected = false
				b.mu.Unlock()
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
	if ask == nil || ask.ts == "" {
		return
	}
	if in.User != b.cfg.Owner || in.MessageTS != ask.ts {
		if !ask.warned {
			ask.warned = true
			log.Printf("ignoring a button click that is not the owner answering the pending question (user %s, message %s)",
				logSafe(in.User, maxLoggedValue), logSafe(in.MessageTS, maxLoggedValue))
		}
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
