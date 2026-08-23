package bridge

import (
	"strings"
)

// normalizeMrkdwn rewrites the handful of GitHub-flavored Markdown constructs
// that models reach for by habit into the mrkdwn Slack actually renders, so a
// reply written as `**done**` reads as bold on the owner's phone instead of
// showing its asterisks.
//
// It is deliberately narrow: bold, links and headings, and nothing else. Every
// rule below refuses when the input is ambiguous rather than guessing, because
// a message that renders slightly plainer than intended is a small loss while
// one the normalizer mangled is unreadable and unrecoverable — the owner never
// sees what was meant. Code stays verbatim for the same reason: a fenced block
// or an inline span is the one place where the asterisks are the content.
func normalizeMrkdwn(s string) string {
	if s == "" {
		return s
	}

	lines := strings.Split(s, "\n")
	inFence := false
	for i, line := range lines {
		if isFenceDelimiter(line) {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		lines[i] = normalizeLine(line)
	}
	return strings.Join(lines, "\n")
}

// isFenceDelimiter reports whether the line opens or closes a fenced code
// block. An unclosed fence therefore protects the rest of the message, which is
// the safer way to be wrong about where a code block ended.
func isFenceDelimiter(line string) bool {
	return strings.HasPrefix(strings.TrimLeft(line, " \t"), "```")
}

func normalizeLine(line string) string {
	content, ok := headingContent(line)
	if !ok {
		return normalizeInline(line)
	}

	// Slack has no headings, so the marker has to go either way; bold is the
	// closest thing to the emphasis it was carrying. A line that already
	// emphasises part of itself is left at its own emphasis rather than wrapped
	// in a second layer that would not nest.
	converted := normalizeInline(content)
	if strings.Contains(converted, "*") {
		return converted
	}
	return "*" + converted + "*"
}

// headingContent returns the text of an ATX heading of level 1-3, if the line
// is one. Deeper headings and a `#` with no space after it (a hashtag, an
// issue reference) are not headings here.
func headingContent(line string) (string, bool) {
	i := 0
	for i < len(line) && i < 3 && line[i] == ' ' {
		i++
	}
	start := i
	for i < len(line) && line[i] == '#' {
		i++
	}
	if level := i - start; level < 1 || level > 3 {
		return "", false
	}
	if i >= len(line) || (line[i] != ' ' && line[i] != '\t') {
		return "", false
	}
	content := strings.TrimSpace(line[i:])
	if content == "" {
		return "", false
	}
	return content, true
}

// normalizeInline walks one line left to right, converting bold and links and
// copying everything else — including whole code spans — through untouched.
//
// One pass rather than a chain of replacements: converted output is never
// re-examined, so a URL that happens to contain asterisks cannot be turned into
// bold after the fact.
func normalizeInline(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	i := 0
	for i < len(s) {
		switch s[i] {
		case '\\':
			// A backslash escape is copied with whatever it escapes, so
			// `\*\*text\*\*` stays literal. Escapes are passed through as
			// written; unescaping them is Slack's business, not ours.
			n := 1
			if i+1 < len(s) {
				n = 2
			}
			b.WriteString(s[i : i+n])
			i += n

		case '`':
			n := runLength(s, i, '`')
			if end := findRun(s, i+n, '`', n); end >= 0 {
				b.WriteString(s[i : end+n])
				i = end + n
				continue
			}
			// No closing run, so this is a stray backtick rather than a span.
			b.WriteString(s[i : i+n])
			i += n

		case '[':
			if label, url, next, ok := parseLink(s, i); ok {
				b.WriteString("<")
				b.WriteString(url)
				b.WriteString("|")
				b.WriteString(normalizeInline(label))
				b.WriteString(">")
				i = next
				continue
			}
			b.WriteByte('[')
			i++

		case '*':
			if content, next, ok := parseDelimited(s, i, "**"); ok {
				b.WriteString("*")
				b.WriteString(normalizeInline(content))
				b.WriteString("*")
				i = next
				continue
			}
			// Copy the whole run, so `*italic*` and `***both***` are left as
			// they were written instead of being re-entered a byte later.
			n := runLength(s, i, '*')
			b.WriteString(s[i : i+n])
			i += n

		case '_':
			if content, next, ok := parseDelimited(s, i, "__"); ok && atWordBoundary(s, i, next) {
				b.WriteString("*")
				b.WriteString(normalizeInline(content))
				b.WriteString("*")
				i = next
				continue
			}
			n := runLength(s, i, '_')
			b.WriteString(s[i : i+n])
			i += n

		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

// parseDelimited matches a `**bold**` or `__bold__` span starting at i and
// returns its content and the index just past the closing delimiter.
//
// The refusals are what keep it safe. A run longer than the delimiter is
// something else (`***`, `___`). Content that is empty, that is padded with
// spaces, or that holds another of the same delimiter character is more likely
// arithmetic or a snippet than emphasis. And content holding an odd number of
// backticks means the closing delimiter was found inside a code span, where it
// was never a delimiter at all.
func parseDelimited(s string, i int, delim string) (string, int, bool) {
	d := len(delim)
	if !strings.HasPrefix(s[i:], delim) || runLength(s, i, delim[0]) != d {
		return "", 0, false
	}

	for j := i + d; j+d <= len(s); j++ {
		if s[j] != delim[0] {
			continue
		}
		n := runLength(s, j, delim[0])
		if n != d {
			j += n - 1
			continue
		}
		content := s[i+d : j]
		switch {
		case content == "",
			strings.TrimSpace(content) != content,
			strings.IndexByte(content, delim[0]) >= 0,
			strings.Count(content, "`")%2 != 0:
			return "", 0, false
		}
		return content, j + d, true
	}
	return "", 0, false
}

// parseLink matches `[label](url)` at i and returns the pieces plus the index
// just past the closing paren.
//
// The URL is read with balanced parens so a Wikipedia-style link survives, and
// the whole thing is refused when either half holds a character that would
// break out of Slack's `<url|label>` form.
func parseLink(s string, i int) (label, url string, next int, ok bool) {
	// An image is left alone: Slack has no inline image syntax to convert it
	// into, and the alt text alone would drop what the line was pointing at.
	if i > 0 && s[i-1] == '!' {
		return "", "", 0, false
	}

	rel := strings.IndexByte(s[i+1:], ']')
	if rel < 0 {
		return "", "", 0, false
	}
	end := i + 1 + rel
	if end+1 >= len(s) || s[end+1] != '(' {
		return "", "", 0, false
	}

	label = s[i+1 : end]
	if strings.TrimSpace(label) == "" || strings.ContainsAny(label, "[<>") {
		return "", "", 0, false
	}

	depth := 0
	j := end + 1
	for ; j < len(s); j++ {
		switch s[j] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 {
			break
		}
	}
	if depth != 0 {
		return "", "", 0, false
	}

	url = s[end+2 : j]
	if url == "" || strings.ContainsAny(url, " \t<>|") {
		return "", "", 0, false
	}
	return label, url, j + 1, true
}

// atWordBoundary reports whether a span is free-standing rather than embedded
// in an identifier, which is what separates `__bold__` from the middle of
// `dunder__name__thing`.
//
// Only ASCII counts as a word character on purpose: Japanese and other
// unspaced scripts have no word breaks to find, and treating their letters as
// word characters would mean `太字__強調__です` could never be emphasised.
func atWordBoundary(s string, start, end int) bool {
	if start > 0 && isASCIIWord(s[start-1]) {
		return false
	}
	if end < len(s) && isASCIIWord(s[end]) {
		return false
	}
	return true
}

func isASCIIWord(c byte) bool {
	return c == '_' ||
		(c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z')
}

// runLength counts the consecutive occurrences of c starting at i.
func runLength(s string, i int, c byte) int {
	n := 0
	for i+n < len(s) && s[i+n] == c {
		n++
	}
	return n
}

// findRun returns the index of the next run of exactly n occurrences of c at or
// after from, or -1. Longer runs are skipped rather than matched short.
func findRun(s string, from int, c byte, n int) int {
	for i := from; i < len(s); i++ {
		if s[i] != c {
			continue
		}
		got := runLength(s, i, c)
		if got == n {
			return i
		}
		i += got - 1
	}
	return -1
}
