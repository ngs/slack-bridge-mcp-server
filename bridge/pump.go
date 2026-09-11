package bridge

import (
	"context"
)

// maxSweep bounds one non-blocking drain of a stream channel. It is comfortably
// past every live buffer, so an ordinary backlog is taken in one pass and only
// a flood — a channel refilled as fast as it is emptied — is interrupted, which
// keeps the pump answering its other channels.
const maxSweep = 512

// pump owns the live connection.
//
// It is the only goroutine that receives from the stream: messages, clicks and
// reactions all arrive here, and everything it takes it applies to the bridge's
// own queues before taking the next thing. Tool calls read those queues and
// never the socket.
//
// One reader is what makes receiving and applying a single step. With two, a
// call could take a mention off the events channel and be descheduled before
// registering the thread it opens, while another call judged a reaction against
// the scope that mention was about to change — and found nothing. The reaction
// was not out of scope; it was early by the width of a goroutine switch. No
// caller can observe that state from here, because no caller is between the two
// halves.
//
// It runs until the connection ends: either the socket closes its channels or
// ctx is cancelled with the session.
func (b *Bridge) pump(ctx context.Context, stream Stream) {
	events := stream.Events()
	clicks := stream.Interactions()
	reactions := reactionsOf(stream)

	for {
		select {
		case <-ctx.Done():
			// The session ended, or this connection was replaced. Either way
			// nothing is reading the socket from here, and a call blocked on a
			// context of its own would otherwise wait out its whole timeout on
			// a stream with no reader.
			b.noteStreamClosed(stream)
			return

		case evt, ok := <-events:
			if !ok {
				b.endStream(stream, events, clicks, reactions)
				return
			}
			b.applyEvent(evt, reactions)

		case in, ok := <-clicks:
			if !ok {
				b.endStream(stream, events, clicks, reactions)
				return
			}
			b.routeInteraction(in)

		case r, ok := <-reactions:
			if !ok {
				b.endStream(stream, events, clicks, reactions)
				return
			}
			b.applyReaction(events, r)
		}
	}
}

// applyEvent applies one message together with any reactions already on the
// wire.
//
// The pair rule works both ways round. A message and a reaction that arrive in
// the same instant land on different channels, and whichever the pump happens
// to pick first, the other is already there: taking them together is what makes
// a delivery that has both hand over both, instead of splitting them across two
// calls on a coin toss.
//
// Messages first within the pair, as always — the mention that opens a
// conversation has to be applied before a reaction is judged against it.
func (b *Bridge) applyEvent(evt StreamEvent, reactions <-chan Reaction) {
	b.mu.Lock()
	b.absorbLocked(evt)
	drainLocked(reactions, maxSweep, b.absorbReactionLocked)
	opens := b.takeThreadOpensLocked()
	b.mu.Unlock()

	b.persistThreadOpens(opens)
}

// applyReaction queues one reaction, applying the messages ahead of it first.
//
// A reaction is judged against the conversations that are open, and the mention
// that opens one is a message. Anything already on the events channel arrived
// before this reaction, so it is applied before the reaction is queued behind
// it — and both happen in the pump's goroutine, so a call draining the queues
// sees the pair or neither, never the reaction without the mention it belongs
// to.
func (b *Bridge) applyReaction(events <-chan StreamEvent, r Reaction) {
	// One lock for both, which is what makes "the pair or neither" true rather
	// than merely likely. Taking the lock twice would leave a call able to
	// drain between them and judge the reaction against a scope the mention it
	// belongs to was about to change.
	b.mu.Lock()
	drainLocked(events, maxSweep, b.absorbLocked)
	b.absorbReactionLocked(r)
	opens := b.takeThreadOpensLocked()
	b.mu.Unlock()

	b.persistThreadOpens(opens)
}

// endStream takes what the dying connection had already delivered, and then
// records that it is over.
//
// Any one of the three channels closing means the connection is finished: the
// socket closes all of them together, in the same deferred chain, so the first
// one a select happens to notice is as good a signal as the last. Which it
// notices is not something a caller should be able to tell.
//
// What is still buffered is taken first, and messages before reactions, for the
// reason the pump reads them in that order at all: a reaction is judged against
// the conversations that are open. It was all received before the socket died,
// which is not the loss the live-only limitation describes — that is what never
// arrived, not what arrived and was thrown away.
func (b *Bridge) endStream(stream Stream, events <-chan StreamEvent, clicks <-chan Interaction, reactions <-chan Reaction) {
	b.mu.Lock()
	// One lock for the whole backlog, for the same reason a reaction and the
	// messages ahead of it share one: a call woken halfway through would hand
	// over the messages and find the reactions that arrived with them only on
	// its next turn, which is the split this design exists to remove.
	drainLocked(events, maxSweep, b.absorbLocked)
	drainLocked(clicks, maxSweep, b.deliverInteraction)
	drainLocked(reactions, maxSweep, b.absorbReactionLocked)
	opens := b.takeThreadOpensLocked()
	b.mu.Unlock()

	b.persistThreadOpens(opens)
	b.noteStreamClosed(stream)
}

// drainLocked applies everything already queued on one channel, without waiting
// for more, up to a bound. The caller holds b.mu, and apply must expect that.
//
// The bound matters because a channel can be refilled as fast as it is emptied:
// an unbounded sweep would hold the lock, and the pump, for as long as the
// flood lasted. The closed check is not a formality either — a closed channel
// is permanently ready, so without it this would spin rather than reach its
// default. What a closed channel still holds is yielded first, so nothing
// received before the close is left behind.
func drainLocked[T any](ch <-chan T, bound int, apply func(T)) {
	for i := 0; i < bound; i++ {
		select {
		case v, ok := <-ch:
			if !ok {
				return
			}
			apply(v)
		default:
			return
		}
	}
}

// streamGone reports whether the connection a call is using has ended.
//
// It is how a blocked call learns what the pump saw. The identity check matters
// as much as the flag: a call that started on a connection since replaced is
// reading a stream nobody is pumping any more, and has to be told so rather
// than wait out its timeout on it.
func (b *Bridge) streamGone(stream Stream) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stream != stream || !b.connected
}
