package bridge

import (
	"context"
	"errors"
)

// ErrThreadUnreadable reports that a thread is not there to be read — the
// parent was deleted, or the bridge is no longer in the channel. It is
// distinguished from every other failure because catch-up answers the two
// differently: a thread that is gone is skipped, while a thread Slack merely
// refused this time is retried, so its replies are not stepped over.
var ErrThreadUnreadable = errors.New("the thread cannot be read")

// ErrMissingScope reports that Slack refused a call because the installed app
// was never granted the scope it needs. It is distinguished from every other
// failure because it is not a fault to retry: a scope appears when the app is
// reinstalled and never before, so the feature that needs it steps aside for
// the rest of the connection instead of failing the calls around it.
var ErrMissingScope = errors.New("the Slack app is missing a scope")

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

// HistoryRequest asks for messages between Oldest and Latest. Both bounds are
// exclusive, which is what Slack does when `inclusive` is left off, and either
// may be empty. Oldest exclusive is load-bearing: it is the cursor, and a
// message the agent already has must not come back.
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
	Cursor   string
	Limit    int
}

// Question is the message slack_ask posts: one line of Markdown and one button
// per option. It is a plain description of the Block Kit payload so the bridge
// never has to name a slack-go type; slackclient.go turns it into blocks.
type Question struct {
	// BlockID identifies the actions block, and comes back on every click.
	BlockID string
	// Text is the question itself, as standard Markdown: it goes into a
	// markdown block, which is Slack's own renderer for it.
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
	// JoinedChannels lists the channels the bot itself is a member of, newest
	// page first and at most limit of them. It is how the search for mentions
	// missed while the session was down knows where to look.
	JoinedChannels(ctx context.Context, limit int) ([]string, error)
	// BotUserID is the user ID of the bot the tokens belong to, learned when
	// the connection was opened. A mention is that ID appearing in the text, so
	// the bridge cannot recognise one without it.
	BotUserID() string
	// Post sends an agent-written message to a channel, optionally into a
	// thread, and returns the timestamp of the posted message. The text is
	// Markdown, and Slack renders it — except when it is too long for that,
	// where an implementation may fall back to sending it as the body, with
	// only Slack's own mrkdwn applied.
	Post(ctx context.Context, channel, threadTS, text string) (string, error)
	// PostPlain sends a message as the body text alone, with no blocks, so a
	// later text-only Update replaces all of what the channel shows. It
	// exists for the bridge's own furniture — the processing indicator — which
	// a block would freeze in place, since an update carrying no blocks leaves
	// the ones already on the message standing. Slack's own mrkdwn still
	// applies to the text; what it does not get is Markdown.
	PostPlain(ctx context.Context, channel, threadTS, text string) (string, error)
	// PostQuestion sends a question with clickable answers and returns the
	// timestamp of the posted message.
	PostQuestion(ctx context.Context, channel, threadTS string, q Question) (string, error)
	// React adds an emoji reaction to a message.
	React(ctx context.Context, channel, ts, emoji string) error
	// Update rewrites the text of a message the bridge posted.
	Update(ctx context.Context, channel, ts, text string) error
	// ResolveQuestion rewrites an answered or expired question and removes
	// its buttons, which is how it stops being clickable. The text is
	// Markdown, rendered as the live question was; an implementation that
	// cannot render it must still drop the buttons.
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
)

// StreamEvent is one item from the live event stream.
type StreamEvent struct {
	Kind    StreamEventKind
	Message Message
}

// Interaction is one button click on a message the bridge posted. It is
// already acknowledged to Slack by the time it reaches the bridge: an
// unacknowledged envelope is redelivered, so the ack belongs next to the
// socket rather than behind the bridge's own routing.
//
// Clicks travel on a channel of their own rather than alongside messages. A
// message the bridge fails to queue is recovered from conversations.history; a
// click is not in any history, so a dropped one is an answer the owner gave
// and the agent will never see. Keeping the two apart means a flood of
// messages cannot cost a question its answer.
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
	// Events delivers messages and connection notices until the stream's
	// context is done, at which point the channel is closed.
	Events() <-chan StreamEvent
	// Interactions delivers button clicks, on a separate channel because they
	// cannot be recovered from history if they are dropped.
	Interactions() <-chan Interaction
}

// Connector opens both halves of the Slack connection. Connect is called
// lazily, on the first slack_wait, and the returned Stream runs until ctx is
// cancelled.
type Connector interface {
	Connect(ctx context.Context, cfg Config) (API, Stream, error)
}
