package bridge

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// waitingFilePrefix names the presence file. One per home channel rather than
// one per process, because a single state directory serves every bridge the
// owner runs — one per workspace — and each of them has its own attendant to
// account for.
const waitingFilePrefix = "waiting-"

// WaiterPresence is the document written to waiting-<channel>.json: how many
// calls in this process are currently listening for the owner.
//
// It exists for something outside the process to read — a Stop hook on the
// session, which fires when a turn ends and needs to know whether the agent
// left anyone listening before it stopped. That makes the shape a contract:
// the field names and the RFC 3339 timestamp are what the hook parses, and
// because every one of its paths fails open, renaming a key here does not break
// the guard loudly. It silently turns it back into a no-op.
type WaiterPresence struct {
	// Waits and Asks are counted apart because they mean different things to a
	// reader: a wait is the attendant listening, an ask is the agent blocked on
	// the owner. Neither is a stopped session, and only the first is the loop
	// that keeps the conversation alive.
	Waits int `json:"waits"`
	Asks  int `json:"asks"`
	// PID identifies the process the counts belong to, so a reader can tell a
	// live bridge from a file left behind by one that was killed.
	PID int `json:"pid"`
	// Updated is when the counts last changed, RFC 3339. It is not a heartbeat:
	// it moves when a call starts or ends and stays put in between, so a reader
	// treating it as one would call a long, healthy wait dead.
	Updated string `json:"updated"`
}

// WaitingFilePath is where the presence file for one home channel lives.
func WaitingFilePath(dir, channel string) string {
	return filepath.Join(dir, waitingFilePrefix+channel+".json")
}

// enterWait and its three siblings are the only things that move the counts.
// They are paired with a defer at the top of each blocking call, so the file
// says what is true even when the call returns down an error path.
func (b *Bridge) enterWait() { b.adjustWaiters(1, 0) }

func (b *Bridge) exitWait() { b.adjustWaiters(-1, 0) }

func (b *Bridge) enterAsk() { b.adjustWaiters(0, 1) }

func (b *Bridge) exitAsk() { b.adjustWaiters(0, -1) }

// adjustWaiters moves the counts and republishes the file.
//
// The counts are floored at zero. They should never go negative — every enter
// has exactly one deferred exit — but a count that did would read as "nobody is
// listening" for every call after it, and the guard that depends on it fails
// open, so the bug would be invisible.
func (b *Bridge) adjustWaiters(waits, asks int) {
	b.mu.Lock()
	b.activeWaits = max(b.activeWaits+waits, 0)
	b.activeAsks = max(b.activeAsks+asks, 0)
	presence := b.presenceLocked()
	b.mu.Unlock()

	b.writePresence(presence)
}

// presenceLocked snapshots the counts. The caller must hold b.mu.
func (b *Bridge) presenceLocked() WaiterPresence {
	return WaiterPresence{
		Waits:   b.activeWaits,
		Asks:    b.activeAsks,
		PID:     os.Getpid(),
		Updated: time.Now().UTC().Format(time.RFC3339),
	}
}

// writePresence publishes the counts, and never lets that failure reach the
// caller: the file is a courtesy to a hook outside the process, and losing it
// must not cost the owner a message. The complaint is made once, because the
// reason it failed — a read-only directory, a full disk — will still be true on
// every wait after this one.
func (b *Bridge) writePresence(presence WaiterPresence) {
	if b.cfg.Channel == "" {
		// An unconfigured bridge has no home channel to name the file after,
		// and the tool call is about to fail with that as its reason.
		return
	}

	dir, err := b.cfg.ResolveStateDir()
	if err == nil {
		err = writeWaitingFile(WaitingFilePath(dir, b.cfg.Channel), presence)
	}
	if err == nil {
		return
	}

	b.mu.Lock()
	warned := b.presenceWarned
	b.presenceWarned = true
	b.mu.Unlock()

	if !warned {
		log.Printf("could not record that this session is listening: %s", logSafe(err.Error(), maxLoggedError))
	}
}

// writeWaitingFile replaces the file through a temporary one in the same
// directory, so a reader arriving mid-write sees the previous counts rather
// than a half-written document. The hook that reads this runs at an arbitrary
// moment by definition — a turn ending has nothing to do with a wait starting —
// so a torn read is not a theoretical race here.
func writeWaitingFile(path string, presence WaiterPresence) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}

	data, err := json.Marshal(presence)
	if err != nil {
		return fmt.Errorf("encoding the waiter counts: %w", err)
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("creating a temporary file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer func() {
		// Harmless once the rename below has succeeded.
		_ = os.Remove(tmpName)
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing %s: %w", tmpName, err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("setting permissions on %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replacing %s: %w", path, err)
	}
	return nil
}
