package bridge

import (
	"sort"
	"strconv"
	"strings"
)

// Message is one owner message handed to the MCP client. It is deliberately
// minimal: the bridge transports text, not Slack's full message model.
type Message struct {
	// TS is the Slack message timestamp, which doubles as its ID within a
	// channel and as the ordering key.
	TS string `json:"ts"`
	// ThreadTS is set when the message is a reply inside a thread, so the
	// caller can reply into the same thread via slack_post. Outside the home
	// channel it is always set: a conversation opened by a mention lives in the
	// thread under the message that opened it.
	ThreadTS string `json:"thread_ts,omitempty"`
	// Channel is where the message was sent. The home channel is only one of
	// the conversations the bridge relays, so a reply has to say which one it
	// belongs to: pass this back to slack_post along with ThreadTS.
	Channel string `json:"channel"`
	// User is the Slack user ID of the author. Always the configured owner.
	User string `json:"user"`
	// Text is the message body as Slack stores it (mrkdwn source). An upload
	// with no caption has none, in which case Files is what the message says.
	Text string `json:"text"`
	// Files describes what the owner attached, and is absent from the message
	// they did not attach anything to. The bridge does not download anything:
	// the caller decides whether an attachment is worth fetching, and fetches
	// it itself from URLPrivate.
	Files []File `json:"files,omitempty"`
}

// File is one attachment, as metadata. Only the fields a caller can act on are
// carried: what it is called, what it is, how big it is, and the two links —
// one to the bytes, one to the message in Slack. Slack's own file object has
// forty more fields, thumbnails and preview HTML among them, and putting those
// in front of a model would cost more context than the attachment is worth.
type File struct {
	Name     string `json:"name,omitempty"`
	Mimetype string `json:"mimetype,omitempty"`
	// Size is the file's length in bytes.
	Size int `json:"size,omitempty"`
	// URLPrivate is where the bytes are, and it is not a public link: fetching
	// it needs the bot token in an Authorization header and the files:read
	// scope. Without the scope the request lands on a login page instead of
	// the file.
	URLPrivate string `json:"url_private,omitempty"`
	// Permalink is the link a person opens in Slack, useful for pointing the
	// owner back at their own attachment.
	Permalink string `json:"permalink,omitempty"`
}

// allowedSubtypes are the message subtypes the bridge relays. A plain channel
// message and a plain thread reply both carry no subtype at all;
// thread_broadcast is a thread reply that the author also sent to the channel,
// and file_share is the owner attaching something, with or without a caption.
// All three are the owner speaking. Everything else (message_changed,
// message_deleted, channel_join, bot_message and so on) is either not new text
// or not the owner.
//
// file_share is a live-stream concern more than a history one: the message
// event for an upload carries the subtype, while the same message read back
// from conversations.history carries no subtype at all and only the files
// array. Accepting both shapes is what keeps an upload from being relayed by
// one path and dropped by the other.
var allowedSubtypes = map[string]bool{
	"":                 true,
	"thread_broadcast": true,
	"file_share":       true,
}

// candidate is the common shape of a message arriving from either source: the
// live Socket Mode event stream or a conversations.history page. Normalising
// both into this struct keeps a single filtering rule for the two paths, so a
// message cannot be relayed live but dropped on catch-up.
type candidate struct {
	Channel  string
	User     string
	BotID    string
	SubType  string
	Text     string
	TS       string
	ThreadTS string
	// Username is the display name carried by posts that have no user to look
	// up: incoming webhooks and bots that set their own name. Only
	// slack_history uses it; the relay filter rejects those posts outright.
	Username string
	// ReplyCount is how many replies hang off this message, which
	// slack_history reports so the caller knows a thread is there to read.
	ReplyCount int
	// LatestReply is the timestamp of the newest reply in this message's
	// thread. It is how catch-up spots a thread that has been talked in since
	// the cursor was last moved, without reading every thread in the channel.
	LatestReply string
	// Files are the attachments the message carries, already narrowed to the
	// fields the bridge reports.
	Files []File
}

// accept reports whether the candidate is owner text worth relaying, returning
// the relayable form. It applies the rules that hold in every channel: the
// owner wrote it, an app did not, it is a plain message, and it says something.
//
// channel restricts the result to one channel, which is what a caller reading a
// known conversation wants. An empty channel accepts any, and is for the live
// stream: which conversations are open changes while the session runs, so that
// decision belongs to the bridge rather than to the socket.
//
// Bot messages are rejected even when they carry the owner's user ID, which is
// what stops the bridge from feeding the agent's own slack_post echoes back to
// it as new input.
func accept(c candidate, channel, owner string) (Message, bool) {
	if owner == "" {
		return Message{}, false
	}
	if channel != "" && c.Channel != "" && c.Channel != channel {
		return Message{}, false
	}
	if c.BotID != "" || c.User != owner {
		return Message{}, false
	}
	if !allowedSubtypes[c.SubType] {
		return Message{}, false
	}
	if c.TS == "" {
		return Message{}, false
	}
	// An upload with no caption is still the owner handing something over, so
	// only a message carrying neither text nor files has nothing to relay.
	if strings.TrimSpace(c.Text) == "" && len(c.Files) == 0 {
		return Message{}, false
	}
	// The channel asked for is the authority when the candidate does not carry
	// one: conversations.history results are scoped to the channel requested,
	// and the caller has to be able to say which conversation a reply belongs
	// to.
	from := c.Channel
	if from == "" {
		from = channel
	}
	return Message{
		TS:       c.TS,
		ThreadTS: c.ThreadTS,
		User:     c.User,
		Text:     c.Text,
		Channel:  from,
		Files:    c.Files,
	}, true
}

// mergeConversations orders messages from several conversations into one
// oldest-first run, dropping duplicates.
//
// Unlike mergeMessages it deduplicates by channel and timestamp together, and
// filters nothing by age: each conversation has its own cursor, and the caller
// has already applied them. A timestamp identifies a message only within its
// channel, so keying on it alone would let a message in one channel hide a
// message in another that happened in the same instant.
func mergeConversations(sources ...[]Message) []Message {
	seen := make(map[string]bool)
	var merged []Message
	for _, source := range sources {
		for _, m := range source {
			if m.TS == "" {
				continue
			}
			key := m.Channel + "\x00" + m.TS
			if seen[key] {
				continue
			}
			seen[key] = true
			merged = append(merged, m)
		}
	}
	sort.SliceStable(merged, func(i, j int) bool {
		return tsLess(merged[i].TS, merged[j].TS)
	})
	return merged
}

// tsLess orders two Slack timestamps. They look like "1723456789.000200": a
// Unix second count and a per-second sequence number. Comparing them as
// strings happens to work while every timestamp has ten integer digits, but
// that is an accident of the current epoch, so the components are compared
// numerically instead. Malformed values fall back to a string comparison so
// ordering stays total rather than panicking on unexpected input.
func tsLess(a, b string) bool {
	as, aq, aok := splitTS(a)
	bs, bq, bok := splitTS(b)
	if !aok || !bok {
		return a < b
	}
	if as != bs {
		return as < bs
	}
	return aq < bq
}

// splitTS breaks "seconds.sequence" into its two integer components.
func splitTS(ts string) (seconds, sequence int64, ok bool) {
	whole, frac, found := strings.Cut(ts, ".")
	if !found {
		frac = "0"
	}
	seconds, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	sequence, err = strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return seconds, sequence, true
}

// mergeMessages combines messages from any number of sources into a single
// oldest-first run, dropping duplicates by timestamp and anything at or before
// after.
//
// Catch-up and the live stream overlap by design: a message can arrive over
// the WebSocket and also appear in the conversations.history page fetched on
// the same reconnect. Deduplicating here means the caller never sees it twice
// no matter how the two races resolve. When after is empty, nothing is
// filtered out by age.
func mergeMessages(after string, sources ...[]Message) []Message {
	seen := make(map[string]bool)
	var merged []Message
	for _, source := range sources {
		for _, m := range source {
			if m.TS == "" || seen[m.TS] {
				continue
			}
			if after != "" && !tsLess(after, m.TS) {
				continue
			}
			seen[m.TS] = true
			merged = append(merged, m)
		}
	}
	sort.SliceStable(merged, func(i, j int) bool {
		return tsLess(merged[i].TS, merged[j].TS)
	})
	return merged
}
