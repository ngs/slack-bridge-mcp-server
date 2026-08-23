//go:build unix

package bridge

import (
	"errors"
	"os"
	"syscall"
)

// lockFile takes an exclusive flock without blocking. flock is released by the
// kernel when the last descriptor for the open file closes, which includes the
// process dying, so no stale-lock cleanup is needed.
func lockFile(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return ErrAlreadyLocked
	}
	return err
}

func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
