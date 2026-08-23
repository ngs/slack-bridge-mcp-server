package bridge

import "context"

// HistoryPage is one page of conversations.history, newest message first, as
// Slack returns it.
type HistoryPage struct {
	Messages   []candidate
	NextCursor string
}

// HistoryRequest asks for messages after Oldest, exclusive.
type HistoryRequest struct {
	Channel string
	Oldest  string
	Cursor  string
	Limit   int
}

// API is the slice of the Slack Web API the bridge uses. It exists so the
// bridge can be tested without a network: the production implementation in
// slackclient.go wraps *slack.Client, and the tests supply a fake.
type API interface {
	// History fetches one page of channel history.
	History(ctx context.Context, req HistoryRequest) (HistoryPage, error)
	// Post sends a message to a channel, optionally into a thread, and
	// returns the timestamp of the posted message.
	Post(ctx context.Context, channel, threadTS, text string) (string, error)
	// React adds an emoji reaction to a message.
	React(ctx context.Context, channel, ts, emoji string) error
}

// StreamEventKind distinguishes the things that can happen on the Socket Mode
// connection that the bridge cares about.
type StreamEventKind int

const (
	// StreamMessage carries one message that passed the owner filter.
	StreamMessage StreamEventKind = iota
	// StreamConnected means the WebSocket (re)connected. The bridge answers
	// it by running catch-up, since anything sent while the socket was down
	// exists only in Slack's history.
	StreamConnected
	// StreamDropped means a message was observed but could not be queued.
	// Nothing is lost: the bridge treats it exactly like a reconnect and
	// re-reads the window from history.
	StreamDropped
)

// StreamEvent is one item from the live event stream.
type StreamEvent struct {
	Kind    StreamEventKind
	Message Message
}

// Stream is the live half of the Slack connection.
type Stream interface {
	// Events delivers stream events until the stream's context is done, at
	// which point the channel is closed.
	Events() <-chan StreamEvent
}

// Connector opens both halves of the Slack connection. Connect is called
// lazily, on the first slack_wait, and the returned Stream runs until ctx is
// cancelled.
type Connector interface {
	Connect(ctx context.Context, cfg Config) (API, Stream, error)
}
