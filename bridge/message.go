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
	// caller can reply into the same thread via slack_post.
	ThreadTS string `json:"thread_ts,omitempty"`
	// User is the Slack user ID of the author. Always the configured owner.
	User string `json:"user"`
	// Text is the message body as Slack stores it (mrkdwn source).
	Text string `json:"text"`
}

// allowedSubtypes are the message subtypes the bridge relays. A plain channel
// message and a plain thread reply both carry no subtype at all;
// thread_broadcast is a thread reply that the author also sent to the channel,
// which is still owner-authored text and worth relaying. Everything else
// (message_changed, message_deleted, channel_join, bot_message, file_share
// notices and so on) is either not new text or not the owner speaking.
var allowedSubtypes = map[string]bool{
	"":                 true,
	"thread_broadcast": true,
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
}

// accept reports whether the candidate is an owner message on the bound
// channel that should be handed to the caller, returning the relayable form.
//
// Bot messages are rejected even when they carry the owner's user ID, which is
// what stops the bridge from feeding the agent's own slack_post echoes back to
// it as new input.
func accept(c candidate, channel, owner string) (Message, bool) {
	if channel == "" || owner == "" {
		return Message{}, false
	}
	if c.Channel != "" && c.Channel != channel {
		return Message{}, false
	}
	if c.BotID != "" || c.User != owner {
		return Message{}, false
	}
	if !allowedSubtypes[c.SubType] {
		return Message{}, false
	}
	if c.TS == "" || strings.TrimSpace(c.Text) == "" {
		return Message{}, false
	}
	return Message{
		TS:       c.TS,
		ThreadTS: c.ThreadTS,
		User:     c.User,
		Text:     c.Text,
	}, true
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
