package bridge

import (
	"context"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"
)

// Bounds on the two passes that recover conversations outside the home
// channel. Both are best effort by design: the home channel is the one with a
// delivery guarantee, and these exist so a mention sent while the laptop was
// asleep is usually found rather than always found.
//
// maxScannedChannels and mentionScanPageLimit bound the search for mentions the
// session slept through: the newest page of each of that many joined channels.
// A mention older than one page of a busy channel is missed, and so is one in
// the twenty-first channel; both are logged.
const (
	maxScannedChannels   = 20
	mentionScanPageLimit = 100
)

// threadKey identifies one open conversation. A thread timestamp is unique
// only within its channel, so both halves are needed.
type threadKey struct {
	channel  string
	threadTS string
}

// mentionsUser reports whether text addresses the given user.
//
// Slack writes a mention as the user's ID in angle brackets, sometimes with a
// display label after a pipe — <@U123> or <@U123|nagase> — so matching the
// bare ID would also fire on a link to a message that happens to contain it,
// and matching only the short form would miss the labelled one.
func mentionsUser(text, userID string) bool {
	if userID == "" {
		return false
	}

	needle := "<@" + userID
	for i := 0; ; {
		at := strings.Index(text[i:], needle)
		if at < 0 {
			return false
		}
		rest := text[i+at+len(needle):]
		if strings.HasPrefix(rest, ">") || strings.HasPrefix(rest, "|") {
			return true
		}
		i += at + len(needle)
	}
}

// classifyLocked decides whether an owner message is part of a conversation
// with the agent, and opens one when the owner asks for it. The caller must
// hold b.mu.
//
// Three rules, in order. The home channel relays everything, which is what it
// has always done and what the owner's own private channel is for. Anywhere
// else, a mention is the owner starting a conversation, and it opens a thread:
// under the message they mentioned in, or the thread they mentioned in if they
// were already in one. And once a thread is open, everything they say in it is
// relayed without further ceremony — which is the point of a thread, and why
// the alternative of demanding a mention every time was rejected.
//
// Everything else is dropped. A channel the bot has been added to is somebody
// else's workspace, not an inbox, and relaying its traffic would put the
// owner's colleagues in the agent's context without anybody asking for it.
func (b *Bridge) classifyLocked(m Message) (Message, bool) {
	if m.Channel == "" || m.Channel == b.cfg.Channel {
		return m, true
	}

	if mentionsUser(m.Text, b.botUserID) {
		thread := m.ThreadTS
		if thread == "" {
			// The mention was made out on the channel surface, so the
			// conversation goes in the thread under it rather than into a
			// channel other people are using.
			thread = m.TS
		}
		b.openThreadLocked(m.Channel, thread)
		m.ThreadTS = thread
		return m, true
	}

	if m.ThreadTS != "" && b.threads[threadKey{m.Channel, m.ThreadTS}] {
		return m, true
	}
	return Message{}, false
}

// openThreadLocked registers a conversation and persists it, so a restart
// resumes it rather than waiting to be mentioned again. The caller must hold
// b.mu.
func (b *Bridge) openThreadLocked(channel, threadTS string) {
	if b.threads == nil {
		b.threads = make(map[threadKey]bool)
	}
	key := threadKey{channel, threadTS}
	if b.threads[key] {
		return
	}
	b.threads[key] = true

	if b.store == nil {
		return
	}
	if err := b.store.SetThread(channel, threadTS, ""); err != nil {
		// The thread still works for this session; only surviving a restart is
		// at risk, and the owner can reopen it with another mention.
		log.Printf("could not persist an open conversation thread: %v", err)
	}
}

// loadThreadsLocked restores the conversations open when the last session
// ended. The caller must hold b.mu.
func (b *Bridge) loadThreadsLocked() error {
	if b.store == nil {
		return nil
	}

	threads, err := b.store.Threads()
	if err != nil {
		return err
	}

	b.threads = make(map[threadKey]bool, len(threads))
	b.threadCursors = make(map[threadKey]string, len(threads))
	for _, t := range threads {
		if t.Channel == "" || t.ThreadTS == "" {
			continue
		}
		key := threadKey{t.Channel, t.ThreadTS}
		b.threads[key] = true
		b.threadCursors[key] = t.LastTS
	}
	return nil
}

// openThreads is the conversations to catch up on, with the cursor each one
// stands at.
func (b *Bridge) openThreads() map[threadKey]string {
	b.mu.Lock()
	defer b.mu.Unlock()

	cursors := make(map[threadKey]string, len(b.threads))
	for key := range b.threads {
		cursors[key] = b.threadCursors[key]
	}
	return cursors
}

// catchUpConversations recovers what was said outside the home channel while
// the bridge was not listening: new mentions first, then the replies in every
// thread that is open once those are counted.
//
// The order matters. A mention found by the scan opens a thread, and the
// replies the owner left under it are only reachable through
// conversations.replies — so the thread pass has to run after the scan, or a
// conversation started while the laptop was asleep would arrive with its
// opening line and nothing else until the next reconnect.
func (b *Bridge) catchUpConversations(ctx context.Context, api API, owner string, generation uint64) ([]Message, error) {
	if api == nil {
		return nil, nil
	}
	if b.conversationsAreDegraded(generation) {
		// Already established that this installation cannot do it. Asking again
		// on every catch-up would spend two API calls a tick to be told the same
		// thing, and say so in the log each time.
		return nil, nil
	}

	mentions, cursor, err := b.scanForMentions(ctx, api, owner)
	if err != nil {
		if !errors.Is(err, ErrMissingScope) {
			return nil, err
		}
		b.degradeConversations(generation, err)
		return nil, nil
	}

	// Opening the threads before the walk is what lets the walk find them, and
	// it records the conversations even if a later step fails, so the owner
	// does not have to mention the bot twice.
	starts := b.openThreads()
	b.mu.Lock()
	for _, m := range mentions {
		b.openThreadLocked(m.Channel, m.ThreadTS)
		// The opening message is already in hand, so the walk starts just after
		// it. The thread's own cursor stays where it is: moving it here would
		// filter out the very message that opened the conversation.
		starts[threadKey{m.Channel, m.ThreadTS}] = m.TS
	}
	b.mu.Unlock()

	replies, err := b.catchUpThreadConversations(ctx, api, owner, starts)
	if err != nil {
		if !errors.Is(err, ErrMissingScope) {
			return nil, err
		}
		// The mentions are already read and are handed over; only the walk
		// through the threads is given up. Their cursors have not moved, so the
		// replies are still there to be found once the app is reinstalled.
		b.degradeConversations(generation, err)
		replies = nil
	}

	if cursor != "" {
		b.noteMentionCursor(cursor)
	}
	return mergeConversations(mentions, replies), nil
}

// conversationsAreDegraded reports whether the catch-up outside the home
// channel has been given up on for this connection.
func (b *Bridge) conversationsAreDegraded(generation uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Generations start at one, so a zero never matches a connection that has
	// actually been refused.
	return generation != 0 && b.degradedGeneration == generation
}

// degradeConversations turns off the catch-up outside the home channel for the
// rest of the connection, and says once what the operator has to do about it.
//
// The alternative — failing the call — is what the bridge used to do, and it
// took an app that had simply not been reinstalled since these scopes were
// added and stopped it receiving anything at all, home channel included. A
// feature nobody has granted permission for is not an error; it is a feature
// that is off.
//
// The generation is the connection the refusal came from. A catch-up runs with
// b.mu released, so a slow one can still be in flight when the connection is
// replaced — and a refusal from the installation as it was must not switch off
// the first scan of a connection whose scopes may well be there.
func (b *Bridge) degradeConversations(generation uint64, err error) {
	b.mu.Lock()
	stale := generation != b.connGeneration
	already := b.degradedGeneration == generation
	if !stale {
		b.degradedGeneration = generation
	}
	b.mu.Unlock()

	if stale || already {
		return
	}
	log.Printf("slack refused a call for want of a scope (%s); conversations outside the home channel are off until the app is reinstalled with channels:read and groups:read, plus channels:history and groups:history for the channels it should read. The home channel is unaffected.",
		logSafe(err.Error(), maxLoggedError))
}

// catchUpThreadConversations reads every open thread from the point it was
// last read to.
func (b *Bridge) catchUpThreadConversations(ctx context.Context, api API, owner string, cursors map[threadKey]string) ([]Message, error) {
	if len(cursors) == 0 {
		return nil, nil
	}

	var (
		messages []Message
		walked   int
		skipped  int
	)
	for key, after := range cursors {
		if walked >= maxThreadsPerCatchUp {
			skipped++
			continue
		}
		walked++

		replies, err := readThread(ctx, api, key.channel, owner, key.threadTS, after)
		if err != nil {
			if !errors.Is(err, ErrThreadUnreadable) {
				// Slack said "not now" rather than "not there", so the cursor
				// stays where it is and the next attempt asks again instead of
				// stepping over replies it never read.
				return nil, err
			}
			// The thread is gone — deleted, or the bot removed from the
			// channel. It will be gone next time too, so the conversation is
			// closed rather than retried forever.
			log.Printf("closing a conversation thread that cannot be read: %s", logSafe(err.Error(), maxLoggedError))
			b.closeThread(key)
			continue
		}
		messages = append(messages, replies...)
	}

	if skipped > 0 {
		log.Printf("catch-up read %d conversation threads and skipped %d; replies in the skipped threads will arrive on a later catch-up",
			walked, skipped)
	}
	return messages, nil
}

// scanForMentions looks for mentions that arrived while the bridge was not
// listening, in the channels it has been added to.
//
// This is the sleep-tolerance story for everywhere except the home channel, and
// it is deliberately bounded rather than exhaustive: the newest page of each of
// the first maxScannedChannels channels. A workspace where the owner mentions
// the bot in the twenty-first channel, or a hundred messages back in a busy
// one, will not find it — and that is a loss the log says out loud, not a
// promise quietly broken. The home channel has real catch-up; this does not.
//
// The very first run seeds the cursor instead of delivering anything, for the
// same reason the channel cursor is seeded: a fresh install should join the
// conversation, not replay every mention in the workspace's history.
func (b *Bridge) scanForMentions(ctx context.Context, api API, owner string) ([]Message, string, error) {
	botID := api.BotUserID()
	if botID == "" {
		// Without knowing its own ID the bridge cannot tell a mention from any
		// other message, so there is nothing to look for.
		return nil, "", nil
	}

	b.mu.Lock()
	home, cursor := b.cfg.Channel, b.mentionCursor
	b.mu.Unlock()

	// One more than the cap, because the home channel is almost always in the
	// list and is skipped below: asking for exactly the cap would quietly make
	// it one channel smaller than it says it is.
	channels, err := api.JoinedChannels(ctx, maxScannedChannels+1)
	if err != nil {
		return nil, "", err
	}

	var (
		found   []Message
		newest  = cursor
		scanned int
		skipped int
	)
	for _, channel := range channels {
		if channel == "" || channel == home {
			continue
		}
		if scanned >= maxScannedChannels {
			skipped++
			continue
		}
		scanned++

		page, err := api.History(ctx, HistoryRequest{Channel: channel, Limit: mentionScanPageLimit})
		if err != nil {
			return nil, "", err
		}

		for _, c := range page.Messages {
			if c.TS != "" && (newest == "" || tsLess(newest, c.TS)) {
				// Every message counts towards the cursor, not just the
				// mentions: what it records is how far this pass has looked.
				newest = c.TS
			}
			if cursor == "" || !tsLess(cursor, c.TS) {
				continue
			}
			msg, ok := accept(c, channel, owner)
			if !ok || !mentionsUser(msg.Text, botID) {
				continue
			}
			if msg.ThreadTS == "" {
				msg.ThreadTS = msg.TS
			}
			found = append(found, msg)
		}
	}

	if skipped > 0 {
		log.Printf("looked for missed mentions in %d channels and skipped %d; a mention in one of those will only be seen when it is repeated",
			scanned, skipped)
	}
	if newest == "" {
		// Not one message in any channel scanned, which is what a workspace the
		// app has just been added to looks like. The moment stands in for it, so
		// that the next scan is a real search rather than another first run:
		// without this the cursor would still be unset, and the first mention
		// the owner sends while the session is down would be read as history and
		// dropped.
		//
		// This is the one place the bridge trusts the local clock against
		// Slack's. It can only skip a message posted between this instant and
		// the next scan, and only if the two clocks disagree — a narrow window,
		// and the alternative is losing a mention outright.
		newest = nowTS()
	}
	if cursor == "" {
		// A first run joins rather than replays.
		return nil, newest, nil
	}
	return found, newest, nil
}

// nowTS renders this moment the way Slack writes a timestamp.
func nowTS() string {
	return strconv.FormatInt(time.Now().Unix(), 10) + ".000000"
}

// closeThread forgets a conversation whose thread no longer exists, on disk as
// well as in memory.
//
// Forgetting it only in memory would mean reading it back on the next connect
// and failing on it again, on every reconnect of every session from then on —
// the thread is not coming back, and neither should the record of it.
func (b *Bridge) closeThread(key threadKey) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.threads, key)
	delete(b.threadCursors, key)

	if b.store == nil {
		return
	}
	if err := b.store.RemoveThread(key.channel, key.threadTS); err != nil {
		log.Printf("could not forget a closed conversation thread: %v", err)
	}
}

// noteMentionCursor records how far the search for mentions has looked.
func (b *Bridge) noteMentionCursor(ts string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceMentionCursorLocked(ts)
}

// advanceMentionCursorLocked moves the mention cursor forward, in memory and on
// disk. The caller must hold b.mu.
func (b *Bridge) advanceMentionCursorLocked(ts string) {
	if ts == "" || (b.mentionCursor != "" && !tsLess(b.mentionCursor, ts)) {
		return
	}
	b.mentionCursor = ts
	if b.store == nil {
		return
	}
	if err := b.store.SetMentionCursor(ts); err != nil {
		// The cost is a repeated scan of a window already read, which the
		// thread cursors then filter out. Not worth failing a delivery over.
		log.Printf("could not persist the mention cursor: %v", err)
	}
}
