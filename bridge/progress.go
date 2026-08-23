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

// ProgressRequest is the status line to show, and which conversation it
// belongs to.
type ProgressRequest struct {
	Text string
	// ThreadTS and Channel place the indicator. They start one here when none
	// is running — an empty channel meaning the home channel — and move a
	// running one that is somewhere else. Leaving both out labels the indicator
	// wherever it already is.
	ThreadTS string
	Channel  string
}

// ProgressResult is what slack_progress returns.
//
// TS is the indicator message the label was attached to, and is left out
// entirely when there is no message to name yet: the indicator posts on its own
// goroutine, so a label given before the first post belongs to a message that
// does not exist at the moment this call returns. A call that moves the
// indicator is one of those, since the message at the new location has still to
// be posted.
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
// channel and threadTS say where the status belongs, and naming one is how a
// running indicator is moved. The indicator starts wherever the owner last
// spoke, which is a guess about what the agent is working on, and it is wrong
// as soon as two topics interleave: the label for one conversation lands under
// the other. So a call that names a different conversation retires the
// indicator where it is and starts a fresh one there, carrying the label and
// the original start time — the elapsed counter measures the turn, not the
// message, and must not reset because the status moved.
//
// Only what the call names is changed: a request with a thread and no channel
// moves within the channel the indicator is already in, since an argument left
// out is not an argument to act on. The exception is a thread that would be
// carried into a different channel, which is dropped — a thread belongs to its
// channel, and means nothing in another one. A call naming nothing keeps
// today's behaviour exactly and labels in place, so a session that never
// leaves one conversation never has to think about any of this.
//
// The call's context is unused, unlike the other tools'. Labelling a running
// indicator is a handful of assignments, and the posts and updates that follow
// belong to the indicator's goroutine, which outlives this call by design and
// runs on the bridge's own context. The one thing here that can take time is
// the lazy connect, and only when there is no indicator to label and no session
// open yet; that runs on the bridge's context too, as it does for every other
// tool.
func (b *Bridge) Progress(_ context.Context, req ProgressRequest) (ProgressResult, error) {
	// Normalized before sanitizing, while the text still has the line breaks a
	// heading marker is recognised by; what survives is then flattened onto the
	// single line the indicator has room for.
	label := sanitizeProgressLabel(normalizeMrkdwn(req.Text))
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
			b.startIndicatorLocked(time.Now(), req.Channel, req.ThreadTS)
		}
		in = b.indicator
		b.mu.Unlock()
	}

	in = b.relocateIndicator(in, req.Channel, req.ThreadTS)

	if in == nil {
		// Nothing to label: either the indicator was turned off while this call
		// was connecting, or the turn ended while it was moving. Same answer as
		// above.
		return ProgressResult{}, nil
	}

	in.setLabel(label)
	return ProgressResult{OK: true, TS: in.messageTS()}, nil
}

// relocateIndicator moves a running indicator to the conversation the call
// named, and returns the indicator to label — the new one if it moved, the
// original if it did not.
//
// The move is a retirement and a fresh start rather than an edit, because a
// Slack message cannot change channel or thread: chat.update addresses a
// message where it already is. Going through startIndicatorLocked is what
// keeps that from being visible as two indicators at once — the outgoing
// message is deleted, and the incoming one is handed its predecessor's done
// channel and waits for it before posting, exactly as a new turn does.
//
// The original start time is carried over. The elapsed counter belongs to the
// work the owner is waiting on, and that work did not restart because the
// status line found a better place to sit.
func (b *Bridge) relocateIndicator(in *indicator, channel, threadTS string) *indicator {
	if in == nil || (channel == "" && threadTS == "") {
		return in
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Something else retired or replaced the indicator while this call was
	// working out what to do with it. Whatever is running now belongs to a
	// turn this call knows nothing about, so it is labelled where it is rather
	// than dragged somewhere on the strength of a stale decision.
	if b.indicator != in {
		return b.indicator
	}

	// An argument left out is not an argument to act on: a thread named
	// without a channel moves within the channel the indicator is already in,
	// not to the home channel.
	target, targetThread := in.channel, in.threadTS
	if channel != "" {
		target = b.channelOr(channel)
	}
	switch {
	case threadTS != "":
		targetThread = threadTS
	case target != in.channel:
		// A thread identifies a message only within its own channel, so the
		// one being carried means nothing in the channel this is moving to —
		// at best Slack refuses the post, at worst it lands under whatever
		// message over there happens to share the timestamp. A move across
		// channels that names no thread goes to the new channel's surface.
		targetThread = ""
	}
	if target == in.channel && targetThread == in.threadTS {
		return in
	}

	b.startIndicatorLocked(in.startedAt, target, targetThread)
	return b.indicator
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
