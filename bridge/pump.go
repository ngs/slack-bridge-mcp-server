package bridge

import (
	"context"
	"time"
)

// maxSweep bounds one non-blocking drain of a stream channel. It is comfortably
// past every live buffer, so an ordinary backlog is taken in one pass and only
// a flood — a channel refilled as fast as it is emptied — is interrupted, which
// keeps the pump answering its other channels.
const maxSweep = 512

// maxClosingSweep bounds the last sweep of the reactions channel, the one taken
// as the connection ends. It is larger than maxSweep — larger than the channel
// the socket fills, so an ordinary close abandons nothing at all — because
// there is no next turn for what is left: a reaction on a dead connection is in
// no history, and nobody will deliver it.
const maxClosingSweep = 4 * maxSweep

// maxTeardownSweep bounds the messages taken from a closing connection
// altogether. The socket is stopping, so the channel empties rather than
// refills, and this is only the promise that a stream which does neither
// cannot hold the lock for ever.
const maxTeardownSweep = 4 * maxClosingSweep

// overflowPollWait is how often the pump asks a quiet stream whether it has
// refused a message it could not announce. A refusal happens when the channel
// is full, and the announcement needs room on that same channel, so a stream
// that then goes quiet holds the news indefinitely.
const overflowPollWait = 250 * time.Millisecond

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

	// Whatever the connection before this one was holding is not this one's.
	b.overflowNoted.Store(false)

	// carried holds reactions taken from the socket that are waiting for the
	// messages ahead of them to be applied.
	var carried []Reaction

	// The stream's overflow flag is not delivered as anything, so it is looked
	// at on a timer as well as on every turn the traffic brings round.
	overflowPoll := time.NewTimer(overflowPollWait)
	defer overflowPoll.Stop()

	for {
		// What the stream has lost, and what it is about to say it lost, both
		// read here rather than left for whichever call happens to look.
		//
		// A refused message is announced as a StreamDropped event, and it can
		// only be announced once the channel that had no room has some: until
		// then the bridge would believe it had missed nothing, and a reaction
		// judged in that window is judged against the conversations a message
		// nobody saw would have opened. Asking the stream directly closes that
		// window.
		//
		// A lost reaction is news in itself, and a wait blocked on a long
		// timeout would otherwise sit out the whole of it before the agent
		// heard that its count was wrong.
		b.noteStreamLosses(generation, stream)

		// Messages first, always. The sweep is bounded, so a channel refilled
		// as fast as it is emptied leaves some behind — and a reaction applied
		// while a mention was still sitting there would be judged against a
		// conversation that had not opened yet. Going round again instead
		// means the reactions are reached only when nothing is waiting ahead
		// of them.
		applied, keep := b.applyReady(generation, events, reactions, carried)
		carried = keep

		// Clicks every time round, not only under a flood of messages. They
		// keep no order with anything, they are the one thing no history can
		// give back — a question whose answer is lost times out — and the
		// buffer they wait in is the smallest of the three. A burst of
		// reactions alone used to leave this untouched: nothing counted them,
		// so the sweep looked idle while it worked, and the clicks behind them
		// waited for a message to arrive.
		b.drainReadyClicks(generation, clicks)

		if applied >= maxSweep {
			// A flood defers the reactions, and nothing else. Clicks have no
			// order to keep with messages and no history to be recovered from,
			// and a shutdown that waited for the flood to end would be a
			// shutdown that did not happen.
			select {
			case <-ctx.Done():
				b.finishStream(generation, stream, events, clicks, reactions, carried)
				return
			default:
			}
			continue
		}

		select {
		case <-ctx.Done():
			// The session ended, or this connection was replaced. What the
			// connection had already delivered is taken first — it was
			// received, and the clicks among it are answers nothing can
			// recover — and then the closure is recorded, so a call blocked on
			// a context of its own is told rather than left waiting on a
			// stream with no reader.
			//
			// The socket is stopping on the same context, and it can still be
			// in the middle of handing something over: a last look is taken
			// once it has closed its channels, or once a short wait says it is
			// not going to.
			b.finishStream(generation, stream, events, clicks, reactions, carried)
			return

		case evt, ok := <-events:
			if !ok {
				b.endStream(generation, stream, nil, events, clicks, reactions, carried, true, streamFinished(stream) != nil)
				return
			}
			carried = b.applyEvent(generation, evt, events, reactions, carried)

		case in, ok := <-clicks:
			if !ok {
				// The clicks close before the events do, and what is still on
				// the events channel is the owner's messages: waited for, not
				// abandoned.
				b.finishStream(generation, stream, events, clicks, reactions, carried)
				return
			}
			b.applyClick(generation, in)

		case <-overflowPoll.C:
			// An overflow is a flag on the stream, not an event: the stream
			// cannot announce one until the channel that had no room has some,
			// and if nothing else ever arrives that moment never comes. So the
			// pump comes back and looks rather than waiting to be woken.
			overflowPoll.Reset(overflowPollWait)

		case r, ok := <-reactions:
			if !ok {
				// The reactions close first of the three, so this is the
				// earliest notice of a connection ending — and the one with
				// the most still to come on the other channels.
				b.finishStream(generation, stream, events, clicks, reactions, carried)
				return
			}
			// Carried rather than applied. The next turn of the loop applies
			// the messages first, and only once a sweep comes back with room
			// to spare — meaning the channel was emptied rather than merely
			// swept — is this reaction let through. A reaction cannot be put
			// back on its channel, so this is how it waits.
			//
			// It waits behind a bound, though. A flood of messages that never
			// lets up would otherwise grow this without limit, and a queue
			// that cannot be emptied is the one place a reaction is better
			// lost and reported than held for ever.
			carried = b.carryOne(carried, r)
		}
	}
}

// noteStreamLosses folds what the stream has lost, and what it has refused but
// not yet been able to announce, into the bridge's own state.
func (b *Bridge) noteStreamLosses(generation uint64, stream Stream) {
	// Asked outside the lock: they are questions for somebody else's
	// implementation of the stream, and one of them clears what it reports.
	lostReactions := streamDroppedReactions(stream)
	overflow := streamPendingOverflow(stream)
	if !lostReactions && !overflow {
		return
	}

	if lostReactions {
		// Before the staleness check, and deliberately. The marker clears when
		// it is read, so a connection replaced between the question and this
		// lock would take the answer with it — and a reaction lost on a
		// connection that has since died is still a reaction the agent's count
		// is missing.
		b.noteReactionsDropped()
	}

	b.underLive(generation, func() { b.noteOverflowLocked(overflow) })
}

// noteOverflowLocked raises the catch-up request an overflow calls for. The
// caller must hold b.mu and must be on the live connection.
func (b *Bridge) noteOverflowLocked(overflow bool) {
	if !overflow {
		return
	}
	if b.overflowNoted.Load() {
		// Already accounted for. The flag stands until the stream can
		// announce it, and raising a hole on every poll would keep moving the
		// epoch out from under the catch-up that is answering this one.
		return
	}
	// Held until the stream announces it. The announcement is a StreamDropped
	// event, and absorbing that one is what clears this: the two are the same
	// refusal, and one refusal is one hole.
	b.overflowNoted.Store(true)

	// A hole, even when a catch-up is already due. The one in flight went to
	// Slack before this message was refused, so it may not be the one that
	// answers for it — and on a stream that then goes quiet there is no
	// StreamDropped event coming to ask again.
	b.requestCatchUpForHoleLocked()
}

// finishStream ends a connection whose events channel has not closed yet. The
// socket closes its channels in order — reactions, then clicks, then events —
// and it can still be in the middle of that, or of handing something over, so
// the events are waited for rather than taken as they stand: what is on that
// channel is the owner's messages, and the closure is not going anywhere.
func (b *Bridge) finishStream(generation uint64, stream Stream, events <-chan StreamEvent, clicks <-chan Interaction, reactions <-chan Reaction, carried []Reaction) {
	// Asked once, here, with no lock held: it is a question for somebody
	// else's implementation, and the answer is wanted twice — for the wait and
	// for what the wait's ending means.
	finished := streamFinished(stream)
	late, closed := awaitStreamClose(events, finished)
	b.endStream(generation, stream, late, events, clicks, reactions, carried, closed, finished != nil)
}

// applyReady applies every message already waiting and, once they are
// exhausted, the reactions: the ones the pump is carrying first, then any on
// the channel behind them. It reports how many messages it took, and whether
// the carried ones went in.
//
// One lock for the whole of it, which is what makes a delivery that has both
// hand over both: a call draining the queues sees the messages and the
// reactions that arrived with them together, or neither yet.
//
// Reaching the bound is how the caller knows there may be more messages. At the
// bound the reactions wait, because a reaction applied ahead of a message still
// on the channel is a reaction judged against a scope that message has not had
// its say in.
func (b *Bridge) applyReady(generation uint64, events <-chan StreamEvent, reactions <-chan Reaction, carried []Reaction) (applied int, keep []Reaction) {
	keep = carried
	b.underLive(generation, func() {
		applied = drainLocked(events, maxSweep, b.absorbLocked)
		// Even at the bound. Reaching it is not proof that more is waiting,
		// and the reaction step is where that question is asked — skipping it
		// here would let go of the lock with reactions deferred behind a
		// channel that is already empty.
		more, left := b.takeReactionsLocked(events, reactions, carried, maxSweep-applied)
		applied += more
		keep = left
	})
	return applied, keep
}

// takeReactionsLocked applies the reactions that are ready, once the messages
// ahead of them are in. It reports how many more messages it applied on the
// way, and what has to be carried to the next turn.
//
// Sweeping the events channel and then the reactions channel is not by itself
// an order. Both are filled by the one goroutine that reads the socket, in the
// order Slack sent things — but a message and its reaction handed over between
// the two sweeps would leave the reaction taken and the message still on its
// channel, and a reaction judged before the mention that opens its
// conversation is a reaction dropped.
//
// So the reactions are taken off the channel first and judged last, with
// another sweep of the events between: anything they came behind is on that
// channel by the time they are on theirs, and this is where it is applied. The
// caller must hold b.mu.
func (b *Bridge) takeReactionsLocked(events <-chan StreamEvent, reactions <-chan Reaction, carried []Reaction, room int) (applied int, keep []Reaction) {
	var ready []Reaction
	drainLocked(reactions, maxPendingReactions, func(r Reaction) { ready = append(ready, r) })

	applied = drainLocked(events, room, b.absorbLocked)
	if applied == room && len(events) > 0 {
		// The quota is spent and the channel still has something in it.
		// Looking rather than taking, because taking would only move the
		// boundary: a receive that emptied the channel would leave the
		// reactions deferred behind a channel with nothing in it, which is the
		// split this is here to prevent — the messages ready for whoever asks
		// next, the reaction waiting for a turn with nothing to do.
		//
		// Messages are still waiting, which is the one thing a reaction may
		// not be judged in front of. They wait together.
		return applied, b.carryLocked(carried, ready)
	}

	// Carried before the channel's, because they were taken first.
	for _, r := range carried {
		b.absorbReactionLocked(r)
	}
	for _, r := range ready {
		b.absorbReactionLocked(r)
	}
	return applied, nil
}

// carryOne holds one reaction over to the next turn, within the bound.
//
// The oldest goes, not the newest, which is what the queue it is waiting for a
// place in does: the newest votes are the ones still worth reading, and the
// marker is what tells the agent the count is short either way.
func (b *Bridge) carryOne(carried []Reaction, r Reaction) []Reaction {
	if len(carried) >= maxPendingReactions {
		b.noteReactionsDropped()
		return append(carried[1:], r)
	}
	return append(carried, r)
}

// carryLocked holds reactions over to the next turn, within the bound. A flood
// of messages that never lets up would otherwise grow this without limit, and a
// queue that cannot be emptied is the one place a reaction is better lost and
// reported than held for ever. The caller must hold b.mu.
func (b *Bridge) carryLocked(carried, ready []Reaction) []Reaction {
	for _, r := range ready {
		if len(carried) >= maxPendingReactions {
			// The oldest goes, as it does everywhere a queue of these is
			// bounded: the newest votes are the ones still worth reading.
			b.noteReactionsDroppedLocked()
			carried = append(carried[1:], r)
			continue
		}
		carried = append(carried, r)
	}
	return carried
}

// drainReadyClicks routes every click already waiting. Clicks keep no order
// with messages, so this is safe at any point — including in the middle of a
// flood, which is the one time it matters.
func (b *Bridge) drainReadyClicks(generation uint64, clicks <-chan Interaction) {
	b.underLive(generation, func() { drainLocked(clicks, maxSweep, b.deliverInteraction) })
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

// underLive runs fn with b.mu held, unless the connection it belongs to has
// been replaced. It reports whether fn ran.
//
// Every "am I still the live connection?" check the pump makes goes through
// here, because the answer has to mean the same thing every time: a replaced
// connection applies nothing, routes nothing, and commits nothing. What it
// must still do is record what it has already observed — a reaction lost on a
// connection that has since died is a reaction the agent's count is missing,
// and the marker that says so clears when it is read. Those go in before this
// is called rather than inside fn; noteStreamLosses is the one that does it.
func (b *Bridge) underLive(generation uint64, fn func()) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stale(generation) {
		return false
	}
	fn()
	return true
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
func (b *Bridge) applyEvent(generation uint64, evt StreamEvent, events <-chan StreamEvent, reactions <-chan Reaction, carried []Reaction) (keep []Reaction) {
	keep = carried
	b.underLive(generation, func() {
		keep = b.applyEventLocked(evt, events, reactions, carried)
	})
	return keep
}

// applyEventLocked is applyEvent once the connection is known to be the live
// one and b.mu is held.
func (b *Bridge) applyEventLocked(evt StreamEvent, events <-chan StreamEvent, reactions <-chan Reaction, carried []Reaction) (keep []Reaction) {
	b.absorbLocked(evt)
	// The rest of the ready events before any reaction, not just this one. The
	// events channel is in order, so a reaction on a mention two places behind
	// the one just taken would otherwise be queued while that mention is still
	// unapplied — and judged against a conversation that has not opened yet.
	applied := drainLocked(events, maxSweep, b.absorbLocked)

	// The carried ones go in here too, under this lock. Leaving them for the
	// next turn would let a call drain the message just applied without the
	// reaction that arrived with it, which is the split this design exists to
	// remove. At the bound as well: whether anything is still waiting is the
	// reaction step's question, and it answers it by looking.
	_, keep = b.takeReactionsLocked(events, reactions, carried, maxSweep-applied)
	return keep
}

// applyClick routes one button click, unless the connection it came from has
// been replaced. A click from a connection nobody owns is an answer to a
// question that is over, and letting it through could answer the next one.
func (b *Bridge) applyClick(generation uint64, in Interaction) {
	b.underLive(generation, func() { b.deliverInteraction(in) })
}

// awaitStreamClose waits, briefly, for the socket to finish closing its
// channels after a cancellation.
//
// The pump and the socket stop on the same context, and the socket can be
// between receiving something and enqueueing it when the pump notices. Without
// this the pump returns first and that last event is neither delivered nor
// counted as lost — and a reaction has no history to be recovered from. The
// wait is short because a socket that has not finished in this long is one
// shutdown should not be held up by; what it was holding is then reported as
// lost, like anything else abandoned.
func awaitStreamClose(events <-chan StreamEvent, finished <-chan struct{}) (late []StreamEvent, closed bool) {
	// A stream that says when it has stopped is worth waiting for: that is the
	// whole point of saying so, and what it is waited for is the reaction
	// queued between its last send and its close. A stream that says nothing
	// gets the short wait, because there is nothing to wait for beyond the
	// channels closing and something has to bound a socket that will not.
	//
	// The longer wait is the one shutdown already spends on the pump, so
	// waiting it out here cannot make a shutdown slower than it was.
	wait := streamCloseWait
	if finished != nil {
		// Long, because there is something definite to wait for — and a step
		// short of the wait shutdown spends on the pump, so a producer that
		// never finishes ends this before Close gives up on it rather than at
		// the same moment. Which of the two logged first was otherwise a
		// matter of scheduling.
		wait = pumpStopWait - streamCloseWait
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	for len(late) < maxSweep {
		select {
		case evt, ok := <-events:
			if !ok {
				return late, true
			}
			// Something was still coming. It is carried to endStream rather
			// than applied here, so it goes in with the rest of the backlog
			// and in the order it arrived.
			late = append(late, evt)
		case <-finished:
			// The producer has said it has stopped, which it does before it
			// closes anything. Whatever is on the channels is all there will
			// ever be, and the sweeps that follow take it.
			return late, true
		case <-deadline.C:
			return late, false
		}
	}

	// The bound was reached, which says the channel was busy rather than that
	// it is still open. One more look, without waiting: a stream that closed
	// after handing over exactly this many is a clean close, and calling it a
	// timeout would report reactions lost that never were.
	select {
	case evt, ok := <-events:
		if !ok {
			return late, true
		}
		late = append(late, evt)
	default:
	}
	select {
	case <-finished:
		return late, true
	default:
	}
	return late, false
}

// streamCloseWait is how long a cancelled pump waits for the socket to finish
// closing its channels.
const streamCloseWait = 250 * time.Millisecond

// endStream takes what the dying connection had already delivered — including
// the reactions the pump was carrying, which are as received as anything on the
// channels — and then records that it is over.
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
func (b *Bridge) endStream(generation uint64, stream Stream, late []StreamEvent, events <-chan StreamEvent, clicks <-chan Interaction, reactions <-chan Reaction, carried []Reaction, closed, saysWhenItStops bool) {
	// Asked once, before the lock, and kept whichever branch this takes: they
	// are questions for somebody else's implementation of the stream — asking
	// one of them clears the answer, so asking twice would lose it, and asking
	// either under b.mu hands the lock the pump and every tool call needs to
	// code this package does not own.
	lost := streamDroppedReactions(stream)
	overflow := streamPendingOverflow(stream)

	b.mu.Lock()
	if lost {
		b.reactionsDropped = true
	}

	// A producer that had not finished when the wait for it ran out can still
	// put a reaction on a channel nobody will read again, and one that fits
	// sets no marker of its own. Only for a stream that says when it stops:
	// one that does not has been waited for as long as it can be, and calling
	// every slow close a loss is how the marker stops meaning anything.
	//
	// Before the staleness check, and that is the whole of why it is here. By
	// the time a connection is being ended it has been replaced already —
	// every path that ends one moves the generation before or as it cancels —
	// so a check below would never run at all.
	//
	// On the way out of a session there may be nobody left to read it: Close
	// is the last thing that happens, and the marker it leaves is for a wait
	// that will not come. It is set anyway, because the alternative is a rule
	// with an exception in it — and because a reconnect, which is the other
	// way here, has a whole session still in front of it.
	if !closed && saysWhenItStops {
		b.reactionsDropped = true
	}

	if b.stale(generation) {
		// A connection that has already been replaced hands nothing over:
		// what is left on its channels belongs to a connection nobody owns,
		// and the replacement's catch-up covers the window for messages.
		//
		// Reactions are the exception, because nothing covers them: anything
		// still on this connection was received and will not be delivered.
		// Only when there is something, though — an ordinary reconnect
		// abandons nothing, and saying otherwise would send the agent to
		// re-read a tally that was never wrong, every time the socket
		// reconnected.
		// And only when something is actually left with it. A close that is
		// merely slow abandons nothing, and saying the count might be short
		// every time the socket took its time is how a marker stops meaning
		// anything — the agent learns to ignore the one signal that says a
		// vote went missing.
		if len(carried) > 0 || len(reactions) > 0 {
			b.reactionsDropped = true
		}
		b.mu.Unlock()
		b.noteStreamClosed(generation, stream)
		return
	}
	// One lock for the whole backlog, for the same reason a reaction and the
	// messages ahead of it share one: a call woken halfway through would hand
	// over the messages and find the reactions that arrived with them only on
	// its next turn, which is the split this design exists to remove.
	// The late ones first: they came off the channel ahead of whatever is
	// still on it.
	for _, evt := range late {
		b.absorbLocked(evt)
	}
	drainLocked(events, maxSweep, b.absorbLocked)
	drainLocked(clicks, maxSweep, b.deliverInteraction)

	// The reactions last, and the messages they came behind swept once more
	// before they are judged, as everywhere else. Carried first among them:
	// those were taken from the socket before the ones still on the channel,
	// and they are the whole reason a reaction is carried rather than dropped
	// when the messages ahead of it are not done.
	//
	// The sweep is bigger here than anywhere else because this is the last
	// read this connection will ever get. A reaction left on the channel is
	// in no history and will be delivered by nobody, so what does not fit is
	// reported as lost rather than quietly abandoned.
	var ready []Reaction
	drainLocked(reactions, maxClosingSweep, func(r Reaction) { ready = append(ready, r) })

	// Swept until the channel is empty rather than once: the socket is
	// stopping, so nothing is refilling this, and every message on it is the
	// owner's. The overall bound is what keeps a stream that never stops from
	// holding the lock for ever.
	swept := 0
	for len(events) > 0 && swept < maxTeardownSweep {
		taken := drainLocked(events, maxClosingSweep, b.absorbLocked)
		if taken == 0 {
			break
		}
		swept += taken
	}

	// The reactions are judged against the messages that came before them, and
	// a message still on the channel is one that has not had its say. If the
	// sweeps above could not empty it, the reactions are reported as lost
	// rather than judged against a connection only half applied — the tally is
	// still readable, and a reaction dropped as out of scope is not.
	//
	// This is also the whole of what a producer that would not finish costs on
	// this path. What it is still holding cannot be seen from here; what it
	// already handed over is on these channels, and everything on them has
	// just been taken. A close that is merely slow therefore says nothing —
	// the reactions in its buffer are delivered, not mourned.
	if len(events) > 0 {
		b.noteReactionsDroppedLocked()
		ready, carried = nil, nil
	}

	for _, r := range carried {
		b.absorbReactionLocked(r)
	}
	for _, r := range ready {
		b.absorbReactionLocked(r)
	}
	if len(reactions) > 0 {
		b.noteReactionsDroppedLocked()
	}

	if overflow && !b.overflowNoted.Swap(false) {
		// An overflow the stream recorded but never had room to announce. The
		// messages it refused are in the window and nowhere else, and the
		// cursor has just moved over the ones that did fit — so the request to
		// read that window again outlives the connection that lost them.
		//
		// Unless the pump already raised it by asking, in which case this is
		// the same refusal seen twice.
		b.requestCatchUpForHoleLocked()
	}
	b.mu.Unlock()
	b.noteStreamClosed(generation, stream)
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
