package bridge

import (
	"strings"
	"testing"
)

func TestLogSafe(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		limit int
		want  string
	}{
		{
			name:  "ordinary text is quoted and otherwise untouched",
			in:    "not-a-number",
			limit: maxLoggedValue,
			want:  `"not-a-number"`,
		},
		{
			// The whole point: a value carrying a newline must not be able to
			// look like a second log entry.
			name:  "a forged log line is flattened",
			in:    "10\nslack-bridge: everything is fine",
			limit: maxLoggedValue,
			want:  `"10slack-bridge: everything is fine"`,
		},
		{
			name:  "carriage returns, tabs and escapes go too",
			in:    "a\rb\tc\x1b[31md\x00e",
			limit: maxLoggedValue,
			want:  `"abc[31mde"`,
		},
		{
			name:  "non-ASCII text survives",
			in:    "十秒",
			limit: maxLoggedValue,
			want:  `"十秒"`,
		},
		{
			name:  "empty stays empty",
			in:    "",
			limit: maxLoggedValue,
			want:  `""`,
		},
		{
			name:  "over the limit is cut and marked",
			in:    "abcdefghij",
			limit: 4,
			want:  `"abcd"…`,
		},
		{
			// The limit counts runes, not bytes, so a multi-byte value is not
			// cut in the middle of a character.
			name:  "the limit counts runes",
			in:    "ありがとうございます",
			limit: 3,
			want:  `"ありが"…`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logSafe(tt.in, tt.limit); got != tt.want {
				t.Errorf("logSafe(%q, %d) = %s, want %s", tt.in, tt.limit, got, tt.want)
			}
		})
	}
}

// Whatever else changes, the result has to be a single line: that is the
// property the sanitizer exists for.
func TestLogSafeIsAlwaysOneLine(t *testing.T) {
	for _, in := range []string{
		"plain",
		"two\nlines",
		"\r\n\r\n",
		strings.Repeat("x\ny", 500),
	} {
		if got := logSafe(in, maxLoggedError); strings.ContainsAny(got, "\r\n") {
			t.Errorf("logSafe(%q, …) = %s, want no line breaks", in, got)
		}
	}
}
