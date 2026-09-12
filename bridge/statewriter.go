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

	flush := func() {
		var failed []stateWrite
		for _, w := range b.takeStateWrites() {
			if err := applyStateWrite(store, w); err != nil {
				b.noteStateWriteError(err)
				failed = append(failed, w)
				continue
			}
			b.noteStateWriteOK()
		}
		if len(failed) > 0 {
			// Kept, not dropped. The file is replaced by a rename, and a
			// rename can be refused for reasons that pass — another process
			// reading it on Windows, most of all — so a cursor that did not
			// land waits and goes again rather than being lost to a moment.
			b.requeueStateWrites(failed)
		}
	}

	for {
		select {
		case <-wake:
			flush()
		case <-stop:
			flush()
			return
		}
	}
}

func applyStateWrite(store *Store, w stateWrite) error {
	var err error
	switch {
	case w.kind == writeThread && w.remove:
		err = store.RemoveThread(w.channel, w.threadTS)
	case w.kind == writeThread:
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

// stateWriteRetryWait is how long a refused write waits before going again. It
// is long enough that a reader holding the file has let go, and short enough
// that a cursor is not left behind for a session's worth of work.
const stateWriteRetryWait = 200 * time.Millisecond

// stopStateWriter tells the writer to flush what it has and stop, and waits
// briefly for it. It must be called without b.mu held.
func (b *Bridge) stopStateWriter() {
	b.mu.Lock()
	stop, done := b.stopStateWrites, b.stateWritesDone
	b.stopStateWrites = nil
	b.stateClosed = true
	b.mu.Unlock()

	if stop == nil {
		return
	}
	close(stop)

	timeout := time.NewTimer(stateWriteFlushWait)
	defer timeout.Stop()
	select {
	case <-done:
	case <-timeout.C:
		log.Printf("gave up waiting for the cursors to reach the state file")
	}
}
