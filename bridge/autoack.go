package bridge

import (
	"context"
	"errors"
	"log"
	"time"
)

// autoAckTimeout bounds one batch of receipt reactions. A receipt is only
// worth anything while it is prompt, so a batch that cannot get through in
// this long has already failed at its job and should stop holding a goroutine
// open.
const autoAckTimeout = 30 * time.Second

// ErrAlreadyReacted reports that the emoji was already on the message. For
// everything the bridge does with reactions that is the desired state, not a
// failure, so it is given a name callers can check for instead of a message
// they would have to match on.
var ErrAlreadyReacted = errors.New("the reaction is already on the message")

// autoAck marks the messages slack_wait is about to return as received, so the
// owner sees 👀 the moment their message reaches the session rather than
// whenever the model gets around to calling slack_ack.
//
// It is deliberately fire-and-forget. The reaction is a courtesy, and making
// the owner's message wait on a second round trip to Slack — or fail because
// that round trip failed — would cost more than the courtesy is worth. The
// goroutine runs on the bridge's own context, since the tool call's is
// cancelled as soon as Wait returns.
func (b *Bridge) autoAck(msgs []Message) {
	if len(msgs) == 0 {
		return
	}

	b.mu.Lock()
	disabled, api, channel, emoji := b.cfg.AutoAckDisabled, b.api, b.cfg.Channel, b.cfg.autoAckEmoji()
	b.mu.Unlock()

	if disabled || api == nil {
		return
	}

	timestamps := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if m.TS != "" {
			timestamps = append(timestamps, m.TS)
		}
	}

	go func() {
		ctx, cancel := context.WithTimeout(b.ctx, autoAckTimeout)
		defer cancel()

		for _, ts := range timestamps {
			err := api.React(ctx, channel, ts, emoji)
			switch {
			case err == nil, errors.Is(err, ErrAlreadyReacted):
				// Already marked is the state we wanted anyway; a backlog
				// replayed after a restart lands here and says nothing.
			case ctx.Err() != nil:
				// The session is going away. The remaining receipts have
				// nobody left to reassure.
				return
			default:
				log.Printf("could not mark a message as received: %s", logSafe(err.Error(), maxLoggedError))
			}
		}
	}()
}
