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
// its newest messages. A thread has to be read from its start — Slack pages it
// forward from the parent — so reaching the end is the only way to know what
// the newest messages are. Only the ones that will be returned are kept, so
// the cost of a long thread is API calls rather than memory, and fifty pages
// is ten thousand replies: past any real conversation, and still bounded.
const maxThreadReadPages = 50

// maxParallelNameLookups is how many users.info calls are in flight at once.
// Enough that a channel full of different people resolves quickly, few enough
// that reading history never looks like an attack on the rate limit.
const maxParallelNameLookups = 8

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
	// Channel is the conversation to read. Empty means the home channel.
	Channel string
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
	// Files describes what was attached to the message, in the same shape a
	// relayed message reports it, and is absent when nothing was.
	Files []File `json:"files,omitempty"`
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
	api, _, err := b.apiForCall()
	if err != nil {
		return HistoryResult{}, err
	}

	channel := b.channelFor(req.Channel)
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

// readThreadWindow reads a thread to the end of the requested window and keeps
// its newest limit messages.
//
// A thread cannot be read from the end: conversations.replies starts at the
// parent and walks forward. Passing the limit straight through would answer
// "read this thread" with its oldest few messages — for a limit of one, the
// parent alone — when what the caller wants is how the discussion ended. So
// the thread is walked to the end and only the tail is kept: pages are
// discarded as they are superseded, so a long thread costs calls, not memory.
func readThreadWindow(ctx context.Context, api API, channel string, req ReadRequest, limit int) ([]candidate, bool, error) {
	var (
		kept    []candidate
		dropped bool
		cursor  string
		hasMore bool
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

		kept = append(kept, replies.Messages...)
		if len(kept) > limit {
			kept = kept[len(kept)-limit:]
			dropped = true
		}

		cursor = replies.NextCursor
		if cursor == "" {
			hasMore = replies.HasMore
			break
		}
	}
	if cursor != "" {
		// A thread with more than ten thousand replies in the window. The tail
		// returned is the newest of what was read rather than of the thread,
		// which is what has_more is there to warn about.
		log.Printf("stopped reading a thread after %d pages; it is longer than slack_history will walk", maxThreadReadPages)
		hasMore = true
	}
	return kept, hasMore || dropped, nil
}

// describe turns raw messages into the reported form, oldest first, with each
// distinct author's name looked up once.
func (b *Bridge) describe(ctx context.Context, api API, messages []candidate) []HistoryMessage {
	names := b.resolveNames(ctx, api, messages)

	out := make([]HistoryMessage, 0, len(messages))
	for _, m := range messages {
		if m.TS == "" {
			continue
		}
		out = append(out, HistoryMessage{
			TS:         m.TS,
			User:       m.User,
			UserName:   authorName(m, names),
			Text:       m.Text,
			ThreadTS:   m.ThreadTS,
			Bot:        m.BotID != "" || m.SubType == "bot_message",
			ReplyCount: m.ReplyCount,
			Files:      m.Files,
		})
	}

	sort.SliceStable(out, func(i, j int) bool { return tsLess(out[i].TS, out[j].TS) })
	return out
}

// authorName works out what to call whoever wrote a message, from names
// already resolved.
func authorName(m candidate, names map[string]string) string {
	// A webhook or app post carries the name it wants to be shown under, which
	// is the only name there is: there is no user to look up.
	if m.Username != "" {
		return m.Username
	}
	if m.User == "" {
		if m.BotID != "" {
			return m.BotID
		}
		return "unknown"
	}
	if name := names[m.User]; name != "" {
		return name
	}
	// Unresolved, which is what the missing users:read scope looks like. The
	// ID is still perfectly readable.
	return m.User
}

// resolveNames looks up every distinct author in one batch.
//
// One at a time would mean a users.info round trip per author in reading
// order — two hundred messages of a busy channel can be dozens of them — so
// the lookups run together, a few at a time. The cap is there because a burst
// of parallel calls is how a Slack app gets rate-limited.
//
// The first failure stops the rest. Failures here are effectively one failure:
// the users:read scope is either granted or it is not, and asking fifty more
// times to be told the same thing only slows the answer down and eats the rate
// limit. Successes are cached on the bridge, so a scope granted later starts
// working without a restart.
func (b *Bridge) resolveNames(ctx context.Context, api API, messages []candidate) map[string]string {
	pending := make([]string, 0, len(messages))
	seen := make(map[string]bool, len(messages))
	for _, m := range messages {
		if m.Username != "" || m.User == "" || seen[m.User] {
			continue
		}
		seen[m.User] = true
		pending = append(pending, m.User)
	}
	if len(pending) == 0 {
		return nil
	}

	var (
		mu      sync.Mutex
		names   = make(map[string]string, len(pending))
		failed  bool
		wg      sync.WaitGroup
		lookups = make(chan struct{}, maxParallelNameLookups)
	)
	for _, id := range pending {
		mu.Lock()
		stop := failed
		mu.Unlock()
		if stop {
			break
		}

		wg.Add(1)
		lookups <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-lookups }()

			name, err := b.names().lookup(ctx, api, id)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if !failed {
					failed = true
					log.Printf("could not resolve display names, falling back to user IDs: %s", logSafe(err.Error(), maxLoggedError))
				}
				return
			}
			names[id] = name
		}()
	}
	wg.Wait()

	return names
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
