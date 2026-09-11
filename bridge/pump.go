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
func (b *Bridge) pump(ctx context.Context, generation uint64, stream Stream) {
	events := stream.Events()
	clicks := stream.Interactions()
	reactions := reactionsOf(stream)

	// carried holds reactions taken from the socket that are waiting for the
	// messages ahead of them to be applied.
	var carried []Reaction

	for {
		// Messages first, always. The sweep is bounded, so a channel refilled
		// as fast as it is emptied leaves some behind — and a reaction applied
		// while a mention was still sitting there would be judged against a
		// conversation that had not opened yet. Going round again instead
		// means the reactions are reached only when nothing is waiting ahead
		// of them.
		if b.applyReadyEvents(generation, events, reactions) == maxSweep {
			// A flood defers the reactions, and nothing else. Clicks have no
			// order to keep with messages and no history to be recovered from,
			// and a shutdown that waited for the flood to end would be a
			// shutdown that did not happen.
			b.drainReadyClicks(generation, clicks)
			select {
			case <-ctx.Done():
				b.endStream(generation, events, clicks, reactions)
				return
			default:
			}
			continue
		}

		// The events channel came back with room to spare, so it is empty and
		// anything carried can go in behind what was just applied.
		if len(carried) > 0 {
			b.applyCarriedReactions(generation, carried)
			carried = nil
		}

		select {
		case <-ctx.Done():
			// The session ended, or this connection was replaced. What the
			// connection had already delivered is taken first — it was
			// received, and the clicks among it are answers nothing can
			// recover — and then the closure is recorded, so a call blocked on
			// a context of its own is told rather than left waiting on a
			// stream with no reader.
			b.endStream(generation, events, clicks, reactions)
			return

		case evt, ok := <-events:
			if !ok {
				b.endStream(generation, events, clicks, reactions)
				return
			}
			b.applyEvent(generation, evt, events, reactions)

		case in, ok := <-clicks:
			if !ok {
				b.endStream(generation, events, clicks, reactions)
				return
			}
			b.applyClick(generation, in)

		case r, ok := <-reactions:
			if !ok {
				b.endStream(generation, events, clicks, reactions)
				return
			}
			// Carried rather than applied. The next turn of the loop applies
			// the messages first, and only once a sweep comes back with room
			// to spare — meaning the channel was emptied rather than merely
			// swept — is this reaction let through. A reaction cannot be put
			// back on its channel, so this is how it waits.
			carried = append(carried, r)
		}
	}
}

// applyReadyEvents applies every message already waiting, with any reactions
// that are ready behind them, and reports how many messages it took. Reaching
// the bound is how the caller knows there may be more.
func (b *Bridge) applyReadyEvents(generation uint64, events <-chan StreamEvent, reactions <-chan Reaction) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stale(generation) {
		return 0
	}

	applied := 0
	drainLocked(events, maxSweep, func(evt StreamEvent) {
		b.absorbLocked(evt)
		applied++
	})
	// Only once the events are exhausted. At the bound there may be more
	// waiting, and a reaction applied ahead of them is a reaction judged
	// against a scope those messages have not had their say in.
	if applied > 0 && applied < maxSweep {
		drainLocked(reactions, maxSweep, b.absorbReactionLocked)
	}
	return applied
}

// drainReadyClicks routes every click already waiting. Clicks keep no order
// with messages, so this is safe at any point — including in the middle of a
// flood, which is the one time it matters.
func (b *Bridge) drainReadyClicks(generation uint64, clicks <-chan Interaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stale(generation) {
		return
	}
	drainLocked(clicks, maxSweep, b.deliverInteraction)
}

// stale reports whether this pump's connection has already been replaced. The
// caller must hold b.mu.
//
// Cancelling a connection does not stop its pump the instant it is called: the
// goroutine may be inside a select with a buffered channel ready. Without this
// it could apply one more event from the old connection after the replacement
// was installed, which is the one thing having a single owner is for. What it
// drops is re-read by the new connection's catch-up.
//
// Identity is the connection's generation rather than the stream itself.
// Stream is an exported interface, and comparing two of them compares their
// dynamic values — which panics outright for an implementation that is not
// comparable, a map or a slice field being enough to do it.
func (b *Bridge) stale(generation uint64) bool {
	return b.connGeneration != generation
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
func (b *Bridge) applyEvent(generation uint64, evt StreamEvent, events <-chan StreamEvent, reactions <-chan Reaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stale(generation) {
		return
	}

	b.absorbLocked(evt)
	// The rest of the ready events before any reaction, not just this one. The
	// events channel is in order, so a reaction on a mention two places behind
	// the one just taken would otherwise be queued while that mention is still
	// unapplied — and judged against a conversation that has not opened yet.
	if drainLocked(events, maxSweep, b.absorbLocked) < maxSweep {
		drainLocked(reactions, maxSweep, b.absorbReactionLocked)
	}
}

// applyCarriedReactions queues reactions the pump took while messages were
// still waiting ahead of them.
//
// It is called only after a sweep found the events channel empty, so by here
// the messages that arrived before these reactions have been applied — which is
// what the ordering promises, and what keeps a vote on a mention from being
// judged before the mention has opened anything.
func (b *Bridge) applyCarriedReactions(generation uint64, reactions []Reaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stale(generation) {
		return
	}
	for _, r := range reactions {
		b.absorbReactionLocked(r)
	}
}

// applyClick routes one button click, unless the connection it came from has
// been replaced. A click from a connection nobody owns is an answer to a
// question that is over, and letting it through could answer the next one.
func (b *Bridge) applyClick(generation uint64, in Interaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stale(generation) {
		return
	}
	b.deliverInteraction(in)
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
func (b *Bridge) endStream(generation uint64, events <-chan StreamEvent, clicks <-chan Interaction, reactions <-chan Reaction) {
	b.mu.Lock()
	if b.stale(generation) {
		// A connection that has already been replaced has nothing to hand
		// over: what is left on its channels belongs to a connection nobody
		// owns, and the replacement's catch-up covers the window anyway.
		b.mu.Unlock()
		return
	}
	// One lock for the whole backlog, for the same reason a reaction and the
	// messages ahead of it share one: a call woken halfway through would hand
	// over the messages and find the reactions that arrived with them only on
	// its next turn, which is the split this design exists to remove.
	drainLocked(events, maxSweep, b.absorbLocked)
	drainLocked(clicks, maxSweep, b.deliverInteraction)
	drainLocked(reactions, maxSweep, b.absorbReactionLocked)
	b.mu.Unlock()
	b.noteStreamClosed(generation)
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
func drainLocked[T any](ch <-chan T, bound int, apply func(T)) int {
	applied := 0
	for applied < bound {
		select {
		case v, ok := <-ch:
			if !ok {
				return applied
			}
			apply(v)
			applied++
		default:
			return applied
		}
	}
	return applied
}

// streamGone reports whether the connection a call is using has ended.
//
// It is how a blocked call learns what the pump saw. The identity check matters
// as much as the flag: a call that started on a connection since replaced is
// waiting on a stream nobody is pumping any more, and has to be told so rather
// than wait out its timeout on it.
func (b *Bridge) streamGone(generation uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stale(generation) || !b.connected
}
