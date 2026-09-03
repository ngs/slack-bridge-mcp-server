package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"
)

// testStateDir is the directory the bridge under test is keeping its state in.
// askBridge does not hand back the config it built, and the presence file lives
// beside state.json, which slack_status does report.
func testStateDir(t *testing.T, b *Bridge) string {
	t.Helper()

	path := b.Status().StateFile
	if path == "" {
		t.Fatal("the bridge reports no state file, so there is nowhere for the presence file to be")
	}
	return filepath.Dir(path)
}

// readPresence returns the presence file as a bare map, so the assertions below
// are about the document on disk rather than about the struct that wrote it.
func readPresence(t *testing.T, dir, channel string) map[string]any {
	t.Helper()

	data, err := os.ReadFile(WaitingFilePath(dir, channel))
	if err != nil {
		t.Fatalf("reading the presence file: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("decoding the presence file %q: %v", data, err)
	}
	return doc
}

func presenceCount(t *testing.T, dir, channel, field string) int {
	t.Helper()

	doc := readPresence(t, dir, channel)
	value, ok := doc[field].(float64)
	if !ok {
		t.Fatalf("presence field %q = %v, want a number", field, doc[field])
	}
	return int(value)
}

// The file is read by a Stop hook outside this process, which means its shape is
// a contract and not an implementation detail. The hook reads `.waits + .asks`
// and parses `.updated` as RFC 3339; renaming a key or changing the timestamp
// format silently turns the guard into a no-op, because every one of its paths
// fails open.
func TestPresenceFileKeepsTheShapeTheStopHookReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)

	// Any call that listens publishes the file; a timing out wait is the
	// cheapest one to make happen.
	if _, err := b.Wait(ctx, 20*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	doc := readPresence(t, testStateDir(t, b), testChannel)

	keys := make([]string, 0, len(doc))
	for k := range doc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if want := []string{"asks", "pid", "updated", "waits"}; !reflect.DeepEqual(keys, want) {
		t.Errorf("presence keys = %v, want exactly %v", keys, want)
	}

	if _, err := time.Parse(time.RFC3339, doc["updated"].(string)); err != nil {
		t.Errorf("updated = %v, want an RFC 3339 timestamp the hook can parse: %v", doc["updated"], err)
	}
	if got := int(doc["pid"].(float64)); got != os.Getpid() {
		t.Errorf("pid = %d, want this process %d", got, os.Getpid())
	}
}

// The counts are what the guard actually decides on, so they have to be true
// while the call is in progress and back to zero the moment it is not.
func TestPresenceCountsAWaitWhileItIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)
	dir := testStateDir(t, b)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := b.Wait(ctx, 400*time.Millisecond); err != nil {
			t.Errorf("Wait() error = %v", err)
		}
	}()

	eventually(t, "the wait to be published", func() bool {
		if _, err := os.Stat(WaitingFilePath(dir, testChannel)); err != nil {
			return false
		}
		return presenceCount(t, dir, testChannel, "waits") == 1
	})
	if got := presenceCount(t, dir, testChannel, "asks"); got != 0 {
		t.Errorf("asks = %d during a wait, want 0; the two are counted apart", got)
	}

	<-done
	if got := presenceCount(t, dir, testChannel, "waits"); got != 0 {
		t.Errorf("waits = %d after the wait returned, want 0", got)
	}
}

// A question is the agent blocked on the owner rather than the attendant
// listening, so it is counted in its own field — but it still means somebody is
// there, which is why the hook adds the two together.
func TestPresenceCountsAnAskApartFromAWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)
	dir := testStateDir(t, b)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: 400 * time.Millisecond}); err != nil {
			t.Errorf("Ask() error = %v", err)
		}
	}()

	eventually(t, "the question to be published", func() bool {
		if _, err := os.Stat(WaitingFilePath(dir, testChannel)); err != nil {
			return false
		}
		return presenceCount(t, dir, testChannel, "asks") == 1
	})
	if got := presenceCount(t, dir, testChannel, "waits"); got != 0 {
		t.Errorf("waits = %d during a question, want 0", got)
	}

	stream.interactions <- click(testOwner, askTS, 0)
	<-done
	if got := presenceCount(t, dir, testChannel, "asks"); got != 0 {
		t.Errorf("asks = %d after the question was answered, want 0", got)
	}
}

// The counts have to survive an error, because a wait that failed is still a
// wait that ended. Leaving one behind would tell the hook somebody is listening
// when the call has already given up.
func TestPresenceClearsAfterAFailedWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	b := New(ctx, cfg, &fakeConnector{err: errors.New("connection refused")})
	t.Cleanup(func() { _ = b.Close() })

	if _, err := b.Wait(ctx, 20*time.Millisecond); err == nil {
		t.Fatal("Wait() = nil error with a connector that fails, want the failure reported")
	}
	if got := presenceCount(t, cfg.StateDir, testChannel, "waits"); got != 0 {
		t.Errorf("waits = %d after a failed wait, want 0", got)
	}
}

// The process is about to exit, so whatever the counts said, nobody is
// listening. A file left reading "one wait" would tell the hook the attendant is
// fine when it is gone.
func TestClosePublishesThatNobodyIsListening(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)
	dir := testStateDir(t, b)

	if _, err := b.Wait(ctx, 20*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := presenceCount(t, dir, testChannel, "waits"); got != 0 {
		t.Errorf("waits = %d after Close, want 0", got)
	}
	if got := presenceCount(t, dir, testChannel, "asks"); got != 0 {
		t.Errorf("asks = %d after Close, want 0", got)
	}
}

// The hook reads at an arbitrary moment — a turn ending has nothing to do with a
// wait starting — so a reader must never catch a half-written document. Writing
// through a temporary file and renaming is what guarantees that, and this checks
// nothing is left behind by it.
func TestPresenceLeavesNoTemporaryFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)
	dir := testStateDir(t, b)

	for i := 0; i < 3; i++ {
		if _, err := b.Wait(ctx, 20*time.Millisecond); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the state directory: %v", err)
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "waiting-" + testChannel + ".json", "state.json", "bridge.lock":
		default:
			t.Errorf("state directory holds %q, want no leftovers from the atomic write", entry.Name())
		}
	}

	// The file names the owner's channel, so it should not be world-readable.
	// Windows has no Unix permission bits (Go reports 0666 regardless), so the
	// check only means something on Unix.
	if runtime.GOOS != "windows" {
		info, err := os.Stat(WaitingFilePath(dir, testChannel))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("presence file mode = %v, want no group or other access", perm)
		}
	}
}

// A bridge with no home channel has no file to name, and the tool call is about
// to fail with that as its reason. Writing a "waiting-.json" would leave a file
// nothing ever reads or cleans up.
func TestPresenceIsNotWrittenWithoutAChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	b := New(ctx, Config{StateDir: dir}, &fakeConnector{})
	t.Cleanup(func() { _ = b.Close() })

	if _, err := b.Wait(ctx, 20*time.Millisecond); err == nil {
		t.Fatal("Wait() = nil error on an unconfigured bridge, want the configuration error")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the state directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("state directory holds %d entries, want none written for a bridge with no channel", len(entries))
	}
}
