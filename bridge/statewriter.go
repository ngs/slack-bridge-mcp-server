package bridge

import (
	"log"
	"time"
)

// stateWrite is one change to the state file, as the writer receives it.
//
// The kinds are the three cursors the bridge keeps: how far the home channel
// has been read, how far each conversation outside it has been read (and that
// it is open at all), and how far the search for missed mentions has looked.
type stateWrite struct {
	kind    stateWriteKind
	channel string
	// threadTS names the conversation for a thread write, and is empty for the
	// other kinds.
	threadTS string
	// ts is the cursor being recorded: a message timestamp, or empty for a
	// conversation that is being opened rather than read.
	ts string
}

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

// stateWriteBuffer is how many changes can be waiting to reach the state file.
// Each is a few bytes of cursor, and the queue only grows behind a disk that has
// stopped answering.
const stateWriteBuffer = 256

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
// The queue is bounded and the send never blocks. A change that does not fit is
// dropped with a line saying so: every one of them costs at most some repeated
// work after a restart, which is the same price a failed write has always had.
func (b *Bridge) recordStateWriteLocked(w stateWrite) {
	if b.store == nil {
		return
	}
	if b.stateWrites == nil {
		b.stateWrites = make(chan stateWrite, stateWriteBuffer)
		b.stopStateWrites = make(chan struct{})
		b.stateWritesDone = make(chan struct{})
		go writeState(b.store, b.stateWrites, b.stopStateWrites, b.stateWritesDone)
	}

	select {
	case b.stateWrites <- w:
	default:
		log.Printf("could not queue a cursor for the state file; it will not survive a restart")
	}
}

// writeState is the only goroutine that writes the state file.
//
// One writer keeps the changes in the order they were made, which matters
// because two of them can name the same cursor, and keeps every one of them off
// the paths that must not wait for a disk.
//
// It drains what is queued before it returns, so a session that ends with
// changes outstanding still records them.
func writeState(store *Store, writes <-chan stateWrite, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	for {
		select {
		case w := <-writes:
			applyStateWrite(store, w)
		case <-stop:
			for {
				select {
				case w := <-writes:
					applyStateWrite(store, w)
				default:
					return
				}
			}
		}
	}
}

func applyStateWrite(store *Store, w stateWrite) {
	var err error
	switch w.kind {
	case writeLastTS:
		err = store.SetLastTS(w.channel, w.ts)
	case writeThread:
		err = store.SetThread(w.channel, w.threadTS, w.ts)
	case writeMentionCursor:
		err = store.SetMentionCursor(w.ts)
	}
	if err != nil {
		// What a lost cursor costs is work repeated after a restart: a window
		// read again, a conversation mentioned into again. Never a message the
		// owner sent, which is in Slack either way.
		log.Printf("could not persist a cursor to the state file: %v", err)
	}
}

// stopStateWriter tells the writer to flush what it has and stop, and waits
// briefly for it. It must be called without b.mu held.
func (b *Bridge) stopStateWriter() {
	b.mu.Lock()
	stop, done := b.stopStateWrites, b.stateWritesDone
	b.stopStateWrites = nil
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
