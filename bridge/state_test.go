package bridge

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreRoundTrip(t *testing.T) {
	store := NewStore(t.TempDir())

	// A store that has never been written is the normal first run, not an
	// error, and must not report a cursor.
	got, err := store.LastTS("C1")
	if err != nil {
		t.Fatalf("LastTS() on a missing file returned error %v, want nil", err)
	}
	if got != "" {
		t.Errorf("LastTS() = %q on a missing file, want empty", got)
	}

	if err := store.SetLastTS("C1", "1723456789.000100"); err != nil {
		t.Fatalf("SetLastTS() error = %v", err)
	}

	got, err = store.LastTS("C1")
	if err != nil {
		t.Fatalf("LastTS() error = %v", err)
	}
	if got != "1723456789.000100" {
		t.Errorf("LastTS() = %q, want 1723456789.000100", got)
	}

	// A second Store over the same directory is what a restarted session sees.
	if got, err := NewStore(filepath.Dir(store.Path())).LastTS("C1"); err != nil || got != "1723456789.000100" {
		t.Errorf("a fresh Store read %q (err %v), want the persisted cursor", got, err)
	}
}

func TestStoreKeepsChannelsSeparate(t *testing.T) {
	store := NewStore(t.TempDir())

	if err := store.SetLastTS("C1", "100.000100"); err != nil {
		t.Fatalf("SetLastTS(C1) error = %v", err)
	}
	if err := store.SetLastTS("C2", "200.000100"); err != nil {
		t.Fatalf("SetLastTS(C2) error = %v", err)
	}

	for channel, want := range map[string]string{"C1": "100.000100", "C2": "200.000100", "C3": ""} {
		got, err := store.LastTS(channel)
		if err != nil {
			t.Fatalf("LastTS(%s) error = %v", channel, err)
		}
		if got != want {
			t.Errorf("LastTS(%s) = %q, want %q", channel, got, want)
		}
	}
}

// The cursor is the only thing preventing the agent from re-reading messages
// it already answered, so a late or out-of-order write must not rewind it.
func TestSetLastTSOnlyMovesForward(t *testing.T) {
	store := NewStore(t.TempDir())

	if err := store.SetLastTS("C1", "1723456789.000200"); err != nil {
		t.Fatalf("SetLastTS() error = %v", err)
	}
	if err := store.SetLastTS("C1", "1723456789.000100"); err != nil {
		t.Fatalf("SetLastTS() with an older value error = %v", err)
	}

	got, _ := store.LastTS("C1")
	if got != "1723456789.000200" {
		t.Errorf("LastTS() = %q after an out-of-order write, want the newer 1723456789.000200", got)
	}

	if err := store.SetLastTS("C1", "1723456790.000000"); err != nil {
		t.Fatalf("SetLastTS() error = %v", err)
	}
	if got, _ := store.LastTS("C1"); got != "1723456790.000000" {
		t.Errorf("LastTS() = %q, want the cursor to advance to 1723456790.000000", got)
	}
}

func TestStoreWritesTheDocumentedShape(t *testing.T) {
	store := NewStore(t.TempDir())
	if err := store.SetLastTS("C0123456789", "1723456789.000100"); err != nil {
		t.Fatalf("SetLastTS() error = %v", err)
	}

	data, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("reading the state file: %v", err)
	}

	var raw struct {
		Channels map[string]struct {
			LastTS string `json:"last_ts"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("the state file is not the documented JSON: %v\n%s", err, data)
	}
	if raw.Channels["C0123456789"].LastTS != "1723456789.000100" {
		t.Errorf("state file = %s; want channels.C0123456789.last_ts to hold the cursor", data)
	}

	// The file holds nothing secret, but it records who the owner talks to,
	// so it should not be world-readable.
	info, err := os.Stat(store.Path())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("state file mode = %v, want no group or other access", perm)
	}
}

func TestLoadRejectsACorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, StateFileName), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}

	// Silently treating a corrupt file as "no cursor" would replay history;
	// the caller should see the problem instead.
	if _, err := NewStore(dir).LastTS("C1"); err == nil {
		t.Error("LastTS() on a corrupt state file = nil error, want a parse error")
	}
}

func TestAcquireLockIsExclusive(t *testing.T) {
	dir := t.TempDir()

	first, err := AcquireLock(dir)
	if err != nil {
		t.Fatalf("AcquireLock() error = %v", err)
	}

	// Two bridges on one channel would split the owner's messages between
	// them; the second must be refused rather than quietly competing.
	if _, err := AcquireLock(dir); err == nil {
		t.Error("a second AcquireLock() succeeded, want ErrAlreadyLocked")
	}

	if err := first.Release(); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	second, err := AcquireLock(dir)
	if err != nil {
		t.Fatalf("AcquireLock() after Release error = %v, want the lock to be reusable", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	// Releasing twice happens on the shutdown path when Close runs after an
	// error already cleaned up.
	if err := second.Release(); err != nil {
		t.Errorf("a second Release() = %v, want it to be a no-op", err)
	}
}
