package bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// StateFileName is the file holding the per-channel cursor, inside the state
// directory resolved by Config.ResolveStateDir.
const StateFileName = "state.json"

// ChannelState is what the bridge remembers about one channel between runs.
type ChannelState struct {
	// LastTS is the timestamp of the newest message already handed to a
	// caller. Catch-up asks Slack for everything after it.
	LastTS string `json:"last_ts"`
}

// ThreadState is one conversation thread opened by a mention, and how far
// through it the bridge has read.
type ThreadState struct {
	Channel  string `json:"channel"`
	ThreadTS string `json:"thread_ts"`
	// LastTS is the newest reply already handed to a caller, so a restart
	// resumes the conversation instead of replaying it.
	LastTS string `json:"last_ts,omitempty"`
}

// State is the on-disk document. Channels is keyed by channel, which is what
// let the home channel be joined by the conversations in Threads without a
// migration.
//
// The fields after Channels were added with mention-driven threads and are all
// optional: a state file written by an older build loads unchanged, and the
// bridge starts with no open threads and an unscanned mention cursor, which is
// exactly the state a first run is in.
type State struct {
	Channels map[string]ChannelState `json:"channels"`
	// Threads are the conversations open outside the home channel.
	Threads []ThreadState `json:"threads,omitempty"`
	// MentionCursor is how far through time the search for mentions the bridge
	// slept through has looked. It is global rather than per channel because a
	// Slack timestamp is a moment, comparable across channels, and the question
	// it answers — "what have I not looked at yet?" — is about time.
	MentionCursor string `json:"mention_cursor,omitempty"`
}

// Store reads and writes the state file. It is safe for concurrent use within
// a process; across processes, the flock taken by Lock is what keeps two
// bridges from interleaving writes.
type Store struct {
	path string
	mu   sync.Mutex
}

// NewStore returns a Store backed by state.json inside dir.
func NewStore(dir string) *Store {
	return &Store{path: filepath.Join(dir, StateFileName)}
}

// Path is the location of the state file, for diagnostics.
func (s *Store) Path() string { return s.path }

// Load reads the whole state document. A missing file is not an error: it is
// the normal first run, and yields an empty State.
func (s *Store) Load() (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *Store) loadLocked() (State, error) {
	empty := State{Channels: map[string]ChannelState{}}

	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return empty, nil
	}
	if err != nil {
		return empty, fmt.Errorf("reading %s: %w", s.path, err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return empty, fmt.Errorf("parsing %s: %w", s.path, err)
	}
	if state.Channels == nil {
		state.Channels = map[string]ChannelState{}
	}
	return state, nil
}

// LastTS returns the persisted cursor for a channel, or "" if the bridge has
// never handed out a message from it.
func (s *Store) LastTS(channel string) (string, error) {
	state, err := s.Load()
	if err != nil {
		return "", err
	}
	return state.Channels[channel].LastTS, nil
}

// SetLastTS advances the cursor for a channel and writes the file. The cursor
// only ever moves forward: an out-of-order or stale value is ignored rather
// than rewinding the bridge into replaying messages the caller already has.
func (s *Store) SetLastTS(channel, ts string) error {
	if channel == "" || ts == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, err := s.loadLocked()
	if err != nil {
		return err
	}
	if current := state.Channels[channel].LastTS; current != "" && !tsLess(current, ts) {
		return nil
	}
	state.Channels[channel] = ChannelState{LastTS: ts}
	return s.saveLocked(state)
}

// Threads returns the conversation threads open outside the home channel.
func (s *Store) Threads() ([]ThreadState, error) {
	state, err := s.Load()
	if err != nil {
		return nil, err
	}
	return state.Threads, nil
}

// SetThread records a thread and its cursor, adding it if it is new. Like the
// channel cursor, a thread's only ever moves forward: a stale value is ignored
// rather than rewinding the bridge into replaying replies it already handed
// over.
func (s *Store) SetThread(channel, threadTS, lastTS string) error {
	if channel == "" || threadTS == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, err := s.loadLocked()
	if err != nil {
		return err
	}

	for i, t := range state.Threads {
		if t.Channel != channel || t.ThreadTS != threadTS {
			continue
		}
		if lastTS == "" || (t.LastTS != "" && !tsLess(t.LastTS, lastTS)) {
			return nil
		}
		state.Threads[i].LastTS = lastTS
		return s.saveLocked(state)
	}

	state.Threads = append(state.Threads, ThreadState{Channel: channel, ThreadTS: threadTS, LastTS: lastTS})
	return s.saveLocked(state)
}

// RemoveThread forgets a conversation for good. It is for a thread that is not
// coming back — deleted, or in a channel the bot has been removed from — where
// leaving the record behind would mean reading it again on every reconnect of
// every session from now on.
func (s *Store) RemoveThread(channel, threadTS string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, err := s.loadLocked()
	if err != nil {
		return err
	}

	kept := state.Threads[:0]
	removed := false
	for _, t := range state.Threads {
		if t.Channel == channel && t.ThreadTS == threadTS {
			removed = true
			continue
		}
		kept = append(kept, t)
	}
	if !removed {
		return nil
	}
	state.Threads = kept
	return s.saveLocked(state)
}

// MentionCursor returns how far the search for missed mentions has looked, or
// "" when it never has.
func (s *Store) MentionCursor() (string, error) {
	state, err := s.Load()
	if err != nil {
		return "", err
	}
	return state.MentionCursor, nil
}

// SetMentionCursor moves the mention cursor forward. Backwards is refused for
// the same reason as everywhere else here: it would mean looking again at
// messages already answered.
func (s *Store) SetMentionCursor(ts string) error {
	if ts == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, err := s.loadLocked()
	if err != nil {
		return err
	}
	if state.MentionCursor != "" && !tsLess(state.MentionCursor, ts) {
		return nil
	}
	state.MentionCursor = ts
	return s.saveLocked(state)
}

// saveLocked writes the document through a temporary file in the same
// directory, so a crash mid-write leaves the previous cursor intact instead of
// a truncated file that would look like a first run.
func (s *Store) saveLocked(state State) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding state: %w", err)
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(dir, StateFileName+".*")
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
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replacing %s: %w", s.path, err)
	}
	return nil
}
