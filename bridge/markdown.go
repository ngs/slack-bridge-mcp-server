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
// Text too long for a markdown block goes as the message body instead, where
// only Slack's mrkdwn applies. Splitting it
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

// plainBody sends the text as the message body itself, with no blocks, as the
// bridge did before markdown blocks: Slack applies its own mrkdwn to it and
// nothing else, so Markdown headings and tables show their markup — but the
// ceiling is tens of thousands of characters rather than 12,000.
//
// mrkdwn is deliberately left on. It is what this path has always done, and
// turning it off would newly break the `*bold*` an owner reading these
// messages already sees rendered.
func plainBody(text string) []slack.MsgOption {
	return []slack.MsgOption{slack.MsgOptionText(text, false)}
}

func fitsMarkdownBlock(text string) bool {
	return utf8.RuneCountInString(text) <= maxMarkdownBlock
}

// maxSectionBlock is what a section block's text holds, a quarter of what a
// markdown block does. It bounds the fallback a refused question takes.
const maxSectionBlock = 3000

func fitsSectionBlock(text string) bool {
	return utf8.RuneCountInString(text) <= maxSectionBlock
}

// rejectedForSize reports whether Slack turned this payload away for being too
// big, which is the one failure a plain-text retry can fix.
//
// msg_too_long says so outright. internal_error is Slack's generic failure and
// is only read that way when the text is big enough that expansion could
// plausibly have pushed it over — otherwise a transient failure whose request
// Slack had already accepted would be retried, posting a reply twice or
// undoing an update that had landed.
func rejectedForSize(err error, text string) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "msg_too_long") {
		return true
	}
	return strings.Contains(msg, "internal_error") && mayExceedExpanded(text)
}

// shortcodeAllowance is the room one expandable character is assumed to take
// once Slack rewrites it as its :shortcode:. Shortcodes are not one width —
// :tada: is seven characters and :slightly_smiling_face: is twenty-three — so
// this is an upper bound rather than an average: guessing low would leave a
// message that is nearly full and holds one emoji looking safe, and a post
// rejected without a retry is a reply the owner never sees.
//
// Guessing high costs nothing that matters. The estimate only decides whether
// an internal_error is worth retrying as plain text, and even at this width a
// short message would need dozens of emoji before it counted as oversized —
// far more than the one status marker that made the previous rule useless.
const shortcodeAllowance = 32

// mayExceedExpanded reports whether the text could pass the budget once Slack
// rewrites what is inside it.
//
// Counting the expandable characters, rather than merely noticing one, is what
// keeps this honest for short text: every resolved question carries a ✅ or a
// ⌛, and a status marker is not why a hundred-character message would be
// refused.
func mayExceedExpanded(text string) bool {
	runes, expandable := 0, 0
	for _, r := range text {
		runes++
		if unicode.Is(unicode.So, r) {
			expandable++
		}
	}
	return runes+expandable*shortcodeAllowance > maxMarkdownBlock
}

// literal renders a string so a markdown block shows the characters it
// actually contains.
//
// A code span rather than an escape set, because the string this exists for —
// the label of the button the owner clicked — was shown as plain_text, where
// nothing at all is markup. Escaping would mean naming every construct that
// could fire: emphasis, autolinks, entities, emoji shortcodes. A code span
// suspends all of them at once.
//
// The fence is one backtick longer than the longest run inside, which is how
// Markdown lets a code span contain backticks of its own.
func literal(s string) string {
	longest, run := 0, 0
	for _, r := range s {
		if r == '`' {
			run++
			longest = max(longest, run)
			continue
		}
		run = 0
	}

	fence := strings.Repeat("`", longest+1)
	// A space keeps a leading or trailing backtick in the content from
	// touching the fence, where it would be read as part of it. Markdown
	// strips one space from each end of a padded span.
	if strings.HasPrefix(s, "`") || strings.HasSuffix(s, "`") {
		return fence + " " + s + " " + fence
	}
	return fence + s + fence
}
