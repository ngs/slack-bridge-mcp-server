package bridge

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// LockFileName is the advisory lock taken by the first slack_wait call, inside
// the state directory.
const LockFileName = "bridge.lock"

// ErrAlreadyLocked is returned when another slack-bridge process already holds
// the lock. Two bridges on one channel would race for the same events and each
// would see only part of the conversation, so the second one refuses to start
// rather than silently stealing messages from the first.
var ErrAlreadyLocked = errors.New("another slack-bridge session is already waiting on this channel")

// Lock is a held advisory file lock. The operating system drops it when the
// process exits, including on a crash, so a stale lock file never blocks the
// next session.
type Lock struct {
	file *os.File
}

// AcquireLock takes an exclusive non-blocking lock on bridge.lock inside dir.
// It returns ErrAlreadyLocked if another process holds it.
func AcquireLock(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating %s: %w", dir, err)
	}

	path := filepath.Join(dir, LockFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}

	if err := lockFile(file); err != nil {
		_ = file.Close()
		if errors.Is(err, ErrAlreadyLocked) {
			return nil, err
		}
		return nil, fmt.Errorf("locking %s: %w", path, err)
	}
	return &Lock{file: file}, nil
}

// Release drops the lock. It is safe to call more than once.
func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	err := unlockFile(file)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}
