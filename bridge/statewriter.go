package bridge

import (
	"log"
	"sort"
	"time"
)

// stateWriteKind names the cursors the bridge keeps in the state file.
type stateWriteKind int

const (
	// writeLastTS records how far the home channel has been read.
	writeLastTS stateWriteKind = iota
	// writeThread records a conversation outside the home channel: that it is
	// open, and how far it has been read.
	writeThread
	// writeMentionCursor records how far the search for mentions has looked.
	writeMentionCursor
)

// stateKey identifies what a change is about, so that two changes to the same
// thing collapse into the later one.
type stateKey struct {
	kind     stateWriteKind
	channel  string
	threadTS string
}

// stateWrite is one change to the state file.
type stateWrite struct {
	stateKey
	// ts is the cursor being recorded: a message timestamp, or empty for a
	// conversation being opened rather than read.
	ts string
	// remove marks a conversation the bridge has given up on, which is a
	// deletion rather than a cursor.
	remove bool
	// reopened marks a conversation opened again after being given up on,
	// where the two changes collapsed into this one. The removal has to happen
	// anyway: SetThread leaves an existing cursor alone when it is given an
	// empty one, so without it the conversation would come back still pointing
	// at where the old one had been read to.
	reopened bool
}

// stateWriteFlushWait is how long Close waits for the writer to finish what it
// has. It is short: the writes are small and local, and a disk that cannot
// manage them in this long is one the process should not hang on.
const stateWriteFlushWait = 2 * time.Second

// recordStateWriteLocked queues a change to the state file. The caller must
// hold b.mu.
//
// Nothing writes the state file from under b.mu, and that is the whole point of
// this. The file is read and rewritten whole, and b.mu is the lock the pump
// holds while it applies what the socket delivered — so a slow disk under it
// stops the only reader of the connection, and the click and reaction buffers
// behind it are the ones nothing can recover.
//
// Changes are held in a map rather than a queue, keyed by what they are about.
// Nothing is ever dropped for want of room: a second cursor for the same
// channel replaces the first, which is exactly what writing them in order would
// have left behind anyway, and the number of distinct things is the number of
// conversations the session has open.
func (b *Bridge) recordStateWriteLocked(w stateWrite) {
	if b.store == nil {
		return
	}
	if b.stateClosed {
		// The writer has flushed and stopped, and this is a call that was
		// still running when the session ended. Starting another writer would
		// outlive the shutdown that was waited for; saying so is the honest
		// end of it, and what is lost is work repeated after a restart.
		log.Printf("the state file is closed; a cursor recorded during shutdown will not survive a restart")
		return
	}
	if b.stateDirty == nil {
		b.stateDirty = make(map[stateKey]stateWrite)
		b.stateWake = make(chan struct{}, 1)
		b.stopStateWrites = make(chan struct{})
		b.stateWritesDone = make(chan struct{})
		go b.writeState(b.store, b.stateWake, b.stopStateWrites, b.stateWritesDone)
	}

	if previous, ok := b.stateDirty[w.stateKey]; ok && !w.remove {
		// Given up on and opened again before either reached the file. The
		// later one wins, as always, but it has to undo the first as well —
		// and the marker survives a third write on top, such as the cursor
		// that follows the first reply in the reopened conversation.
		w.reopened = previous.remove || previous.reopened
	}
	b.stateDirty[w.stateKey] = w
	select {
	case b.stateWake <- struct{}{}:
	default:
		// Already awake. One signal and two mean the same instruction, which
		// is to come and take whatever is there.
	}
}

// takeStateWrites hands back everything waiting, leaving the map empty.
func (b *Bridge) takeStateWrites() []stateWrite {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.stateDirty) == 0 {
		return nil
	}
	writes := make([]stateWrite, 0, len(b.stateDirty))
	for _, w := range b.stateDirty {
		writes = append(writes, w)
	}
	b.stateDirty = make(map[stateKey]stateWrite)

	// Conversations before cursors. A mention opens a conversation and moves
	// the mention cursor past itself, and in that order the two survive a crash
	// between them: the conversation is restored and the mention is read again
	// at worst. The other way round, a restart skips the mention and never
	// learns the conversation was open.
	sort.SliceStable(writes, func(i, j int) bool {
		return writes[i].kind == writeThread && writes[j].kind != writeThread
	})
	return writes
}

// writeState is the only goroutine that writes the state file.
//
// One writer keeps the changes off every path that must not wait for a disk,
// and makes the file's contents the business of one place rather than five.
//
// It writes what is waiting before it returns, so a session that ends with
// changes outstanding still records them.
func (b *Bridge) writeState(store *Store, wake <-chan struct{}, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	// flush reports whether everything waiting reached the file.
	flush := func() bool {
		writes := b.takeStateWrites()
		for i, w := range writes {
			if b.stateFenced.Load() {
				// Another session owns the file now. What is left is not
				// written, and what it would have recorded is work done again
				// after a restart — which is the smaller of the two costs.
				b.requeueStateWrites(writes[i:])
				return false
			}
			if err := applyStateWrite(store, w); err != nil {
				b.noteStateWriteError(err)
				// The batch stops here. What follows was ordered behind this
				// write for a reason — a conversation is recorded before the
				// cursor that moves past the mention which opened it — and
				// carrying on would persist the second without the first.
				//
				// Kept, not dropped: the file is replaced by a rename, and a
				// rename can be refused for reasons that pass, so a cursor
				// that did not land waits and goes again.
				b.requeueStateWrites(writes[i:])
				return false
			}
			b.noteStateWriteOK()
		}
		return true
	}

	for {
		select {
		case <-wake:
			flush()
			//nolint:errcheck // a failed flush requeues itself and is woken again
		case <-stop:
			// The last flush, with a few attempts: after this there is no
			// writer left to answer the retry it would otherwise schedule, and
			// what is left would be lost to a refusal that would have passed.
			for i := 0; i < stateWriteFinalAttempts; i++ {
				if flush() {
					return
				}
				time.Sleep(stateWriteRetryWait)
			}
			b.reportUnwrittenState()
			return
		}
	}
}

// fenceStateWriter tells the writer to stop writing, whatever it is in the
// middle of. It is the last resort before the single-instance lock is released:
// another session is about to own this file, and a write from this one landing
// afterwards would overwrite what that session has since recorded.
func (b *Bridge) fenceStateWriter() {
	b.stateFenced.Store(true)
}

func applyStateWrite(store *Store, w stateWrite) error {
	var err error
	switch {
	case w.kind == writeThread && w.remove:
		err = store.RemoveThread(w.channel, w.threadTS)
	case w.kind == writeThread:
		if w.reopened {
			// Clears the cursor the old conversation left behind, which
			// SetThread would otherwise keep.
			if err = store.RemoveThread(w.channel, w.threadTS); err != nil {
				break
			}
		}
		err = store.SetThread(w.channel, w.threadTS, w.ts)
	case w.kind == writeLastTS:
		err = store.SetLastTS(w.channel, w.ts)
	case w.kind == writeMentionCursor:
		err = store.SetMentionCursor(w.ts)
	}
	return err
}

// requeueStateWrites puts back what did not land, unless something newer has
// taken its place, and asks for another attempt after a pause.
func (b *Bridge) requeueStateWrites(failed []stateWrite) {
	b.mu.Lock()
	if b.stateDirty == nil {
		// The writer is on its way out and nobody will read this again.
		b.mu.Unlock()
		return
	}
	for _, w := range failed {
		if _, newer := b.stateDirty[w.stateKey]; !newer {
			if previous, ok := b.stateDirty[w.stateKey]; ok && previous.remove && !w.remove {
				// Given up on and opened again before either reached the file. The
				// later one wins, as always, but it has to undo the first as well.
				w.reopened = true
			}
			b.stateDirty[w.stateKey] = w
		}
	}
	wake := b.stateWake
	b.mu.Unlock()

	time.AfterFunc(stateWriteRetryWait, func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
}

// noteStateWriteError keeps a failing state file to one line per episode. The
// retry means a disk that has stopped answering would otherwise say so for
// every cursor, for as long as it lasted.
func (b *Bridge) noteStateWriteError(err error) {
	b.mu.Lock()
	first := !b.stateWriteFailing
	b.stateWriteFailing = true
	b.mu.Unlock()

	if first {
		// What a cursor that never lands costs is work repeated after a
		// restart: a window read again, a conversation mentioned into again.
		// Never a message the owner sent, which is in Slack either way.
		log.Printf("could not persist a cursor to the state file, and will keep trying: %v", err)
	}
}

func (b *Bridge) noteStateWriteOK() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stateWriteFailing = false
}

// stateWriteFinalAttempts is how many times shutdown tries to write what is
// left. A refusal that passes does so in a moment; one that does not is a disk
// the process should not hang on.
const stateWriteFinalAttempts = 3

// reportUnwrittenState says what did not reach the file, so a restart doing the
// work again is explained rather than mysterious.
func (b *Bridge) reportUnwrittenState() {
	b.mu.Lock()
	left := len(b.stateDirty)
	b.mu.Unlock()

	if left > 0 {
		log.Printf("%d cursor(s) never reached the state file; the work they record will be done again after a restart", left)
	}
}

// stateWriteRetryWait is how long a refused write waits before going again. It
// is long enough that a reader holding the file has let go, and short enough
// that a cursor is not left behind for a session's worth of work.
const stateWriteRetryWait = 200 * time.Millisecond

// stopStateWriter tells the writer to flush what it has and stop, and waits
// briefly for it. It reports whether the writer actually stopped, and must be
// called without b.mu held.
func (b *Bridge) stopStateWriter() bool {
	b.mu.Lock()
	stop, done := b.stopStateWrites, b.stateWritesDone
	b.stopStateWrites = nil
	b.stateClosed = true
	b.mu.Unlock()

	if stop == nil {
		return true
	}
	close(stop)

	timeout := time.NewTimer(stateWriteFlushWait)
	defer timeout.Stop()
	select {
	case <-done:
		return true
	case <-timeout.C:
	}

	// It has had its time. From here nothing more may be written, because the
	// lock that keeps two sessions off this file is about to be let go: the
	// fence stops the next write, and a moment is given to whichever one is
	// already in flight.
	b.fenceStateWriter()

	settle := time.NewTimer(stateWriteFenceWait)
	defer settle.Stop()
	select {
	case <-done:
	case <-settle.C:
	}
	log.Printf("gave up waiting for the cursors to reach the state file")
	return false
}

// stateWriteFenceWait is how long shutdown gives a write already in flight to
// finish, once the writer has been told to stop. One file write is all it can
// be.
const stateWriteFenceWait = time.Second
