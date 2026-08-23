package bridge

import (
	"strings"
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

// rejectedForSize reports whether Slack turned a payload away for being too
// big. It answers to both of the errors an oversized markdown block draws:
// msg_too_long when the count alone settles it, and internal_error when the
// text only exceeds the budget once Slack expands what is inside it.
//
// Retrying an internal_error is safe here because it is only ever asked about
// a rejected markdown post, and a rejected post is one that did not happen.
func rejectedForSize(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "msg_too_long") || strings.Contains(msg, "internal_error")
}
