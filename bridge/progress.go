package bridge

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// maxProgressLabel caps the status line. It shares one line in a Slack message
// with a stopwatch, on a phone, so anything past a sentence or two stops being
// a glance and starts being a paragraph.
const maxProgressLabel = 200

// ProgressResult is what slack_progress returns.
//
// TS is the indicator message the label was attached to, and is left out
// entirely when there is no message to name yet: the indicator posts on its own
// goroutine, so a label given before the first post belongs to a message that
// does not exist at the moment this call returns.
type ProgressResult struct {
	OK bool   `json:"ok"`
	TS string `json:"ts,omitempty"`
}

// Progress tells the channel what the agent is waiting on, as a label beside
// the elapsed time: "⏳ Working… (4m 10s) — release chain: waiting for CI".
//
// The server owns the display from there: the label rides every subsequent
// update and disappears with the indicator, so the agent says this once when it
// starts a long piece of work rather than keeping a message alive by hand.
//
// There are three states to meet. A posted indicator is updated on the spot.
// One still inside its grace period posts immediately instead of waiting it
// out — the agent has just said this will take a while, so there is no quick
// answer left to keep quiet for. And when no indicator is running at all, which
// is where an agent that starts long work after a timed-out wait finds itself,
// this starts one; it then retires like any other, on the next reply or wait.
//
// threadTS only decides where a newly started indicator goes. An indicator that
// already exists belongs to a turn that has already chosen its surface, and
// moving it would mean deleting and reposting the message the owner is watching.
//
// The call's context is unused, unlike the other tools'. Labelling a running
// indicator is a handful of assignments, and the posts and updates that follow
// belong to the indicator's goroutine, which outlives this call by design and
// runs on the bridge's own context. The one thing here that can take time is
// the lazy connect, and only when there is no indicator to label and no session
// open yet; that runs on the bridge's context too, as it does for every other
// tool.
func (b *Bridge) Progress(_ context.Context, text, threadTS string) (ProgressResult, error) {
	label := sanitizeProgressLabel(text)
	if label == "" {
		return ProgressResult{}, errors.New("text is required")
	}

	b.mu.Lock()
	// Nowhere to put a label, so there is nothing to do and no reason to open a
	// Slack connection finding that out. Said as ok:false rather than as an
	// error: the operator turned the indicator off, nothing is broken, and a
	// status line they chose not to have is not worth failing a call the agent
	// made in good faith.
	if b.cfg.IndicatorDisabled {
		b.mu.Unlock()
		return ProgressResult{}, nil
	}
	in := b.indicator
	b.mu.Unlock()

	if in == nil {
		// Starting one needs an API handle, which on the first call of a
		// session means opening the connection.
		if _, _, err := b.apiForCall(); err != nil {
			return ProgressResult{}, err
		}

		b.mu.Lock()
		if b.indicator == nil {
			// Counting from now, because now is when the agent said it was
			// working: whatever happened before this call is not what the owner
			// is being asked to wait for.
			b.startIndicatorLocked(time.Now(), threadTS)
		}
		in = b.indicator
		b.mu.Unlock()
	}

	if in == nil {
		// Nothing was started, which at this point means the indicator was
		// turned off while this call was connecting. Same answer as above.
		return ProgressResult{}, nil
	}

	in.setLabel(label)
	return ProgressResult{OK: true, TS: in.messageTS()}, nil
}

// sanitizeProgressLabel renders the agent's status line as one short line of
// text.
//
// The label is dropped into a message the server owns, next to the elapsed
// time, so it has to stay on that line and read as what it claims to be. Every
// rune that is whitespace or not printable becomes a separator — newlines and
// control characters, but zero-width and bidi marks too, which do not take up a
// line but can hide or reorder what is on it. Runs of them collapse to one
// space, and a label longer than the cap is cut with an ellipsis, so a model
// that answers with a paragraph or pastes a stack trace costs the owner one
// line rather than a wall of text.
func sanitizeProgressLabel(s string) string {
	var b strings.Builder
	space := false
	kept := 0

	for _, r := range s {
		if unicode.IsSpace(r) || !unicode.IsPrint(r) {
			// A leading space is nothing to separate, so start collapsing only
			// once something has been written.
			space = b.Len() > 0
			continue
		}
		if space {
			b.WriteRune(' ')
			kept++
			space = false
		}
		b.WriteRune(r)
		kept++

		if kept > maxProgressLabel {
			// One rune past the cap already settles it, and the rest of
			// whatever this is — a stack trace, a log file — does not need
			// copying to find that out.
			break
		}
	}

	return truncateRunes(b.String(), maxProgressLabel)
}

// truncateRunes shortens s to at most limit runes, ending in an ellipsis when
// anything was dropped. Counting runes rather than bytes is what keeps a label
// written in Japanese from being cut to a fraction of an English one — or cut
// through the middle of a character.
func truncateRunes(s string, limit int) string {
	if utf8.RuneCountInString(s) <= limit {
		return s
	}

	// The ellipsis takes the last of the budget, so the result is never longer
	// than the caller allowed for.
	kept := 0
	for i := range s {
		if kept == limit-1 {
			return s[:i] + "…"
		}
		kept++
	}
	return s
}
