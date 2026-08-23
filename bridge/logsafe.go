package bridge

import (
	"strconv"
	"strings"
	"unicode"
)

// Length caps for logged values. Both are generous enough to identify what
// went wrong and short enough that a pathological value — an environment
// variable holding a megabyte of junk, or a Slack error carrying a whole
// response body — cannot flood the session's stderr.
const (
	maxLoggedValue = 64
	maxLoggedError = 200
)

// logSafe renders an untrusted string for a log line: control characters are
// dropped, the result is truncated, and what is left is quoted.
//
// Nothing reaching the logger here is under the bridge's control — environment
// values come from whoever started the session, Slack error text comes from
// Slack — and a log line is read by people and by whatever tails the CLI's
// stderr. A value containing a newline could forge a second log entry, so the
// characters that would let it are removed rather than escaped.
func logSafe(s string, limit int) string {
	var b strings.Builder
	kept := 0
	truncated := false

	for _, r := range s {
		if kept >= limit {
			truncated = true
			break
		}
		// Printable includes letters, digits, punctuation, symbols and the
		// plain space, which covers anything worth reading back; newlines,
		// carriage returns, tabs and the rest of the control range do not.
		if !unicode.IsPrint(r) {
			continue
		}
		b.WriteRune(r)
		kept++
	}

	quoted := strconv.Quote(b.String())
	if truncated {
		return quoted + "…"
	}
	return quoted
}
