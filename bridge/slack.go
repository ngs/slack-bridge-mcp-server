package bridge

import "context"

// HistoryPage is one page of conversations.history, newest message first, as
// Slack returns it.
type HistoryPage struct {
	Messages   []candidate
	NextCursor string
	// HasMore reports that Slack held messages back from this page, which
	// slack_history passes on so the caller knows it is reading a window
	// rather than everything.
	HasMore bool
}

// HistoryRequest asks for messages between Oldest and Latest, exclusive of
// Oldest. Either bound may be empty.
type HistoryRequest struct {
	Channel string
	Oldest  string
	Latest  string
	Cursor  string
	Limit   int
}

// RepliesRequest asks for the replies to one thread.
type RepliesRequest struct {
	Channel string
	// ThreadTS is the parent message's timestamp, which Slack also returns as
	// the first message of the thread.
	ThreadTS string
	Oldest   string
	Latest   string
	Limit    int
}

// Question is the message slack_ask posts: one line of mrkdwn and one button
// per option. It is a plain description of the Block Kit payload so the bridge
// never has to name a slack-go type; slackclient.go turns it into blocks.
type Question struct {
	// BlockID identifies the actions block, and comes back on every click.
	BlockID string
	// Text is the question itself, as Slack mrkdwn.
	Text string
	// Options are the buttons, in the order the owner sees them.
	Options []QuestionOption
}

// QuestionOption is one button of a Question.
type QuestionOption struct {
	ActionID string
	Value    string
	Label    string
}

// API is the slice of the Slack Web API the bridge uses. It exists so the
// bridge can be tested without a network: the production implementation in
// slackclient.go wraps *slack.Client, and the tests supply a fake.
type API interface {
	// History fetches one page of channel history.
	History(ctx context.Context, req HistoryRequest) (HistoryPage, error)
	// Replies fetches one thread, parent message included.
	Replies(ctx context.Context, req RepliesRequest) (HistoryPage, error)
	// UserName resolves a Slack user ID to the name a person would recognise.
	UserName(ctx context.Context, userID string) (string, error)
	// Post sends a message to a channel, optionally into a thread, and
	// returns the timestamp of the posted message.
	Post(ctx context.Context, channel, threadTS, text string) (string, error)
	// PostQuestion sends a question with clickable answers and returns the
	// timestamp of the posted message.
	PostQuestion(ctx context.Context, channel, threadTS string, q Question) (string, error)
	// React adds an emoji reaction to a message.
	React(ctx context.Context, channel, ts, emoji string) error
	// Update rewrites the text of a message the bridge posted.
	Update(ctx context.Context, channel, ts, text string) error
	// ResolveQuestion rewrites a question to plain text and removes its
	// buttons, which is how an answered or expired question stops being
	// clickable.
	ResolveQuestion(ctx context.Context, channel, ts, text string) error
	// Delete removes a message the bridge posted.
	Delete(ctx context.Context, channel, ts string) error
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
	// StreamInteraction carries a Block Kit button click. Unlike a message it
	// is not filtered by author on the way in: who clicked is part of the
	// payload, and the pending question is what decides whether that user's
	// click counts.
	StreamInteraction
)

// StreamEvent is one item from the live event stream.
type StreamEvent struct {
	Kind        StreamEventKind
	Message     Message
	Interaction Interaction
}

// Interaction is one button click on a message the bridge posted. It is
// already acknowledged to Slack by the time it reaches the bridge: an
// unacknowledged envelope is redelivered, so the ack belongs next to the
// socket rather than behind the bridge's own routing.
type Interaction struct {
	// User is the Slack user ID of whoever clicked, which the bridge checks
	// against the configured owner.
	User string
	// Channel is where the clicked message lives.
	Channel string
	// MessageTS identifies the message the button belongs to, which is how a
	// click is matched to the question that is waiting for it.
	MessageTS string
	// BlockID and ActionID are the identifiers the bridge put on the block
	// and the button when it posted the question.
	BlockID  string
	ActionID string
	// Value is the button's value, which carries the option index.
	Value string
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
