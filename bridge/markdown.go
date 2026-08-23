package bridge

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/slack-go/slack"
)

// maxMarkdownBlock is the character budget Slack allows across the markdown
// blocks of one message. Measured in code points, not bytes: 12,000 Japanese
// characters go through where 12,001 do not, though they are three bytes each.
const maxMarkdownBlock = 12000

// markdownBody renders agent-written text as a Slack markdown block, which
// takes standard Markdown and renders it — headings, tables, links and all —
// instead of the mrkdwn dialect that would show `**bold**` with its asterisks.
//
// The same text is also sent as the message's `text`. With blocks present that
// field is the notification fallback, and it is what the owner's lock screen
// shows, so leaving it out would turn every push notification into an empty
// one.
//
// Text too long for a markdown block goes as plain text instead. Splitting it
// across several blocks would not help, because the budget is for the whole
// message rather than for each block, and splitting it across several messages
// would change what a single slack_post means — one call, one ts, one thing to
// reply to.
func markdownBody(text string) []slack.MsgOption {
	if !fitsMarkdownBlock(text) {
		return plainBody(text)
	}
	return []slack.MsgOption{
		slack.MsgOptionText(text, false),
		slack.MsgOptionBlocks(slack.NewMarkdownBlock("", text)),
	}
}

// plainBody sends the text as the message body itself, as the bridge did
// before markdown blocks: no rendering, but a ceiling in the tens of thousands
// of characters rather than 12,000.
func plainBody(text string) []slack.MsgOption {
	return []slack.MsgOption{slack.MsgOptionText(text, false)}
}

func fitsMarkdownBlock(text string) bool {
	return utf8.RuneCountInString(text) <= maxMarkdownBlock
}

// rejectedForSize reports whether Slack turned this payload away for being too
// big, which is the one failure a plain-text retry can fix.
//
// msg_too_long says so outright. internal_error is Slack's generic failure and
// is only read as a size rejection when the text holds something Slack expands
// — that is the case where a payload inside the character budget is refused
// with nothing more specific to go on. Reading every internal_error that way
// would mean retrying a transient failure whose first request Slack may have
// accepted, and posting the reply twice.
func rejectedForSize(err error, text string) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "msg_too_long") {
		return true
	}
	return strings.Contains(msg, "internal_error") && expandsInSlack(text)
}

// expandsInSlack reports whether the text holds characters Slack rewrites on
// the way in, an emoji becoming its :shortcode:. That expansion is measured
// against the budget, which is how 1,000 emoji fit a markdown block and 1,500
// do not, well short of 12,000 characters.
func expandsInSlack(text string) bool {
	for _, r := range text {
		if unicode.Is(unicode.So, r) {
			return true
		}
	}
	return false
}

// escapeMarkdown neutralises the inline Markdown in a string that was never
// meant to be Markdown, so it reads as the characters it actually contains.
//
// It exists for text that was shown somewhere Markdown is not rendered and is
// later quoted somewhere it is — an answer's button label, which is plain_text
// on the button, appearing in the resolved question. Only the characters that
// start inline formatting are escaped: the ones that matter solely at the
// start of a line cannot fire mid-sentence, and escaping them would leave
// backslashes on show for nothing.
func escapeMarkdown(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if strings.ContainsRune(`\*_`+"`"+`~[]`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
