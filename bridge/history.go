package bridge

import (
	"context"
	"log"
	"sort"
	"sync"
)

// Bounds on one slack_history read. The default is a screenful of
// conversation; the ceiling is Slack's own page size, and also about as much
// channel text as is worth putting into a model's context in one go.
const (
	MinHistoryLimit     = 1
	MaxHistoryLimit     = 200
	DefaultHistoryLimit = 50
)

// maxThreadReadPages caps how much of a thread slack_history will walk to find
// its newest messages. Five pages is a thousand replies, which is a longer
// thread than anyone summarises in one go.
const maxThreadReadPages = 5

// ReadRequest is what slack_history asks for. Every field is optional.
type ReadRequest struct {
	// Limit caps how many messages come back, newest-end first if the window
	// holds more.
	Limit int
	// Oldest and Latest bound the window, as Slack timestamps. Both are
	// exclusive, matching conversations.history: a message whose ts equals a
	// bound is not returned.
	Oldest string
	Latest string
	// ThreadTS reads that thread's replies instead of the channel surface.
	ThreadTS string
}

// HistoryMessage is one message as slack_history reports it. Unlike Message,
// which is the relay's shape and only ever carries the owner, this describes
// whoever wrote it — including bots and incoming webhooks.
type HistoryMessage struct {
	TS string `json:"ts"`
	// User is the author's Slack user ID, absent on webhook posts that have no
	// user behind them.
	User string `json:"user,omitempty"`
	// UserName is the name a person would recognise, resolved from the ID or
	// taken from the name a bot posted under. It falls back to the raw ID.
	UserName string `json:"user_name"`
	Text     string `json:"text"`
	ThreadTS string `json:"thread_ts,omitempty"`
	// Bot marks a post written by an app rather than a person.
	Bot bool `json:"bot"`
	// ReplyCount is set on messages that have a thread hanging off them, so
	// the caller knows there is more to read behind this one.
	ReplyCount int `json:"reply_count,omitempty"`
}

// HistoryResult is what slack_history returns.
type HistoryResult struct {
	Messages []HistoryMessage `json:"messages"`
	// HasMore reports that the window was cut short, either by Slack or by the
	// limit.
	HasMore bool `json:"has_more"`
}

// History reads the channel — or one thread of it — and returns what everyone
// said, not just the owner.
//
// It exists for the one thing the relay cannot do: the owner asks the agent to
// read a discussion it was not part of. That makes it deliberately inert. It
// does not move the cursor, does not touch the pending backlog, does not start
// or stop the indicator, and does not react to anything. Calling it changes
// nothing about what the next slack_wait will deliver.
func (b *Bridge) History(ctx context.Context, req ReadRequest) (HistoryResult, error) {
	api, channel, err := b.apiForCall()
	if err != nil {
		return HistoryResult{}, err
	}

	limit := clampHistoryLimit(req.Limit)

	var (
		messages []candidate
		hasMore  bool
	)
	if req.ThreadTS != "" {
		messages, hasMore, err = readThreadWindow(ctx, api, channel, req, limit)
	} else {
		var page HistoryPage
		page, err = api.History(ctx, HistoryRequest{
			Channel: channel,
			Oldest:  req.Oldest,
			Latest:  req.Latest,
			Limit:   limit,
		})
		messages, hasMore = page.Messages, page.HasMore
		// History comes back newest-first, so the front of the page is the
		// end of the conversation. Slack's limit is a request rather than a
		// promise, and trimming here keeps the contract the tool advertises.
		if len(messages) > limit {
			messages = messages[:limit]
			hasMore = true
		}
	}
	if err != nil {
		return HistoryResult{}, err
	}

	result := HistoryResult{
		Messages: b.describe(ctx, api, messages),
		HasMore:  hasMore,
	}
	return result, nil
}

// readThreadWindow reads a thread and keeps its newest limit messages.
//
// A thread cannot be read from the end: conversations.replies starts at the
// parent and walks forward. Passing the limit straight through would answer
// "read this thread" with its oldest few messages — for a limit of one, the
// parent alone — when what the caller wants is how the discussion ended. So
// the thread is paged and the tail kept.
func readThreadWindow(ctx context.Context, api API, channel string, req ReadRequest, limit int) ([]candidate, bool, error) {
	var (
		messages []candidate
		cursor   string
		hasMore  bool
	)
	for page := 0; page < maxThreadReadPages; page++ {
		replies, err := api.Replies(ctx, RepliesRequest{
			Channel:  channel,
			ThreadTS: req.ThreadTS,
			Oldest:   req.Oldest,
			Latest:   req.Latest,
			Cursor:   cursor,
			Limit:    historyPageLimit,
		})
		if err != nil {
			return nil, false, err
		}

		messages = append(messages, replies.Messages...)
		cursor = replies.NextCursor
		if cursor == "" {
			hasMore = hasMore || replies.HasMore
			break
		}
	}
	// A thread longer than the page budget is read from its start, so the tail
	// kept here is the newest of what was read rather than of the thread. Say
	// so through has_more, which is what it is for.
	if cursor != "" {
		hasMore = true
	}
	if len(messages) > limit {
		messages = messages[len(messages)-limit:]
		hasMore = true
	}
	return messages, hasMore, nil
}

// describe turns raw messages into the reported form, oldest first, resolving
// each distinct author's name once.
func (b *Bridge) describe(ctx context.Context, api API, messages []candidate) []HistoryMessage {
	// A name Slack refuses this time is not worth asking about again within
	// the same call; the shared cache keeps only the answers that worked, so a
	// scope granted later still takes effect without a restart.
	failed := make(map[string]bool)

	out := make([]HistoryMessage, 0, len(messages))
	for _, m := range messages {
		if m.TS == "" {
			continue
		}
		out = append(out, HistoryMessage{
			TS:         m.TS,
			User:       m.User,
			UserName:   b.authorName(ctx, api, m, failed),
			Text:       m.Text,
			ThreadTS:   m.ThreadTS,
			Bot:        m.BotID != "" || m.SubType == "bot_message",
			ReplyCount: m.ReplyCount,
		})
	}

	sort.SliceStable(out, func(i, j int) bool { return tsLess(out[i].TS, out[j].TS) })
	return out
}

// authorName works out what to call whoever wrote a message.
func (b *Bridge) authorName(ctx context.Context, api API, m candidate, failed map[string]bool) string {
	// A webhook post has no user to look up and carries the name it wants to
	// be shown under, which is the only name there is.
	if m.Username != "" {
		return m.Username
	}
	if m.User == "" {
		if m.BotID != "" {
			return m.BotID
		}
		return "unknown"
	}
	if failed[m.User] {
		return m.User
	}

	name, err := b.names().lookup(ctx, api, m.User)
	if err != nil {
		// Most likely the users:read scope is missing, which is a setup
		// choice rather than a fault: the IDs are still readable, so the tool
		// keeps working with them.
		failed[m.User] = true
		log.Printf("could not resolve a display name, using the user ID: %s", logSafe(err.Error(), maxLoggedError))
		return m.User
	}
	return name
}

// names returns the bridge's display-name cache, creating it on first use.
func (b *Bridge) names() *nameCache {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.nameCache == nil {
		b.nameCache = &nameCache{names: make(map[string]string)}
	}
	return b.nameCache
}

// nameCache remembers user IDs the bridge has already resolved. Display names
// change rarely, a session is short-lived, and the alternative is a users.info
// call per message in a busy channel.
type nameCache struct {
	mu    sync.Mutex
	names map[string]string
}

// lookup returns the cached name or fetches it. Two callers racing on the same
// unknown ID both fetch, which costs one extra call and keeps the Slack round
// trip out from under the lock.
func (c *nameCache) lookup(ctx context.Context, api API, userID string) (string, error) {
	c.mu.Lock()
	name, ok := c.names[userID]
	c.mu.Unlock()
	if ok {
		return name, nil
	}

	name, err := api.UserName(ctx, userID)
	if err != nil {
		return "", err
	}
	if name == "" {
		name = userID
	}

	c.mu.Lock()
	c.names[userID] = name
	c.mu.Unlock()
	return name, nil
}

// clampHistoryLimit keeps a caller's limit inside the supported range rather
// than rejecting it, the way the wait timeout is handled.
func clampHistoryLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultHistoryLimit
	case limit < MinHistoryLimit:
		return MinHistoryLimit
	case limit > MaxHistoryLimit:
		return MaxHistoryLimit
	default:
		return limit
	}
}
