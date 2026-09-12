package bridge

import (
	"context"
	"errors"
	"log"
	"sort"
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
	b.recordStateWriteLocked(stateWrite{stateKey: stateKey{kind: writeThread, channel: channel, threadTS: threadTS}})
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

// skippedThreadKeys reports the conversations the last walk ran out of budget
// for, so the next one starts with them.
func (b *Bridge) skippedThreadKeys() map[threadKey]struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.skippedThreads) == 0 {
		return nil
	}
	waiting := make(map[threadKey]struct{}, len(b.skippedThreads))
	for key := range b.skippedThreads {
		waiting[key] = struct{}{}
	}
	return waiting
}

// catchUpSkippedThreads walks the open conversations and nothing else. It is
// what a walk that ran out of budget asks for: the home channel's window and
// the search for mentions were read by the catch-up that skipped them, and
// reading either again would spend two API calls a wake to learn nothing.
func (b *Bridge) catchUpSkippedThreads(ctx context.Context, api API, owner string, generation uint64, scan *scanChanges) ([]Message, error) {
	if api == nil {
		return nil, nil
	}
	if b.conversationsAreDegraded(generation) {
		return nil, nil
	}

	replies, err := b.catchUpThreadConversations(ctx, api, owner, b.openThreads(), b.skippedThreadKeys(), scan)
	if err != nil {
		if !errors.Is(err, ErrMissingScope) {
			return nil, err
		}
		b.degradeConversations(generation, err)
		return nil, nil
	}
	return replies, nil
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
func (b *Bridge) catchUpConversations(ctx context.Context, api API, owner string, generation uint64, scan *scanChanges) ([]Message, error) {
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

	// The walk needs to know which conversations to read, including the ones
	// these mentions open. It is told here, in a map of its own: what the scan
	// would change about the bridge is staged rather than applied, and handed
	// back for the call that delivers to commit under its own generation check.
	// A scan speaks for the installation it ran with, and a reinstall is
	// exactly what changes which threads are readable and which mentions are
	// visible.
	starts := b.openThreads()
	// The earliest mention in each conversation this scan opens. The opening
	// message is already in hand, so the walk starts just after it — but only
	// for a conversation that is new here, and only from the first mention in
	// it.
	//
	// A conversation that was already open keeps the cursor it has. Starting it
	// at a mention found now would step over every reply between where it had
	// been read to and that mention, and the walk's own cursor moves past them
	// when the batch is delivered: they would be skipped for good. And a
	// conversation with two mentions in one scan starts at the first of them,
	// or the replies between the two go the same way.
	opened := make(map[threadKey]string, len(mentions))
	for _, m := range mentions {
		key := threadKey{m.Channel, m.ThreadTS}
		scan.opened = append(scan.opened, key)
		if _, already := starts[key]; already {
			continue
		}
		if first, seen := opened[key]; !seen || tsLess(m.TS, first) {
			opened[key] = m.TS
		}
	}
	for key, first := range opened {
		starts[key] = first
	}

	replies, err := b.catchUpThreadConversations(ctx, api, owner, starts, b.skippedThreadKeys(), scan)
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

	scan.mentionCursor = cursor
	return mergeConversations(mentions, replies), nil
}

// scanChanges is what a catch-up outside the home channel would change about
// the bridge: the conversations it found, the ones it gave up on, and how far
// it looked.
//
// It is staged rather than applied because the scan takes as long as Slack
// takes to answer, and the connection can be replaced underneath it. Everything
// here is true of the installation the scan ran with; a reinstall is what makes
// a thread readable or a mention visible, so the replacement gets to find out
// for itself. The call that delivers commits these under its own generation
// check, or drops them with the batch.
type scanChanges struct {
	opened        []threadKey
	closed        []threadKey
	mentionCursor string
	// skippedKeys are the conversations this walk ran out of budget for. The
	// next walk starts with them.
	skippedKeys []threadKey
	// skipped marks conversations left unread for want of budget. What they
	// hold is still there, and only another catch-up will go and get it.
	skipped bool
}

// commitScanLocked applies what the scan found. The caller must hold b.mu, and
// must have established that its generation is still the current one.
func (b *Bridge) commitScanLocked(scan *scanChanges) {
	if scan == nil {
		return
	}
	for _, key := range scan.opened {
		b.openThreadLocked(key.channel, key.threadTS)
	}
	for _, key := range scan.closed {
		b.closeThreadLocked(key)
	}
	b.advanceMentionCursorLocked(scan.mentionCursor)
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
	defer b.mu.Unlock()

	if generation != b.connGeneration || b.degradedGeneration == generation {
		return
	}
	b.degradedGeneration = generation

	// Logged while the lock is still held, so the line cannot outlive the
	// decision it reports: a connection replaced in the gap would leave this
	// notice describing an installation that is no longer the one running.
	log.Printf("slack refused a call for want of a scope (%s); conversations outside the home channel are off until the app is reinstalled with channels:read and groups:read, plus channels:history and groups:history for the channels it should read. The home channel is unaffected.",
		logSafe(err.Error(), maxLoggedError))
}

// catchUpThreadConversations reads every open thread from the point it was
// last read to.
func (b *Bridge) catchUpThreadConversations(ctx context.Context, api API, owner string, cursors map[threadKey]string, waiting map[threadKey]struct{}, scan *scanChanges) ([]Message, error) {
	if len(cursors) == 0 {
		return nil, nil
	}

	var (
		messages []Message
		walked   int
	)
	for _, key := range threadWalkOrder(cursors, waiting) {
		after := cursors[key]
		if walked >= maxThreadsPerCatchUp {
			// Recorded rather than counted: the next walk starts with these,
			// so a conversation cannot be passed over for ever by a map that
			// happens to yield it last.
			scan.skippedKeys = append(scan.skippedKeys, key)
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
			scan.closed = append(scan.closed, key)
			continue
		}
		messages = append(messages, replies...)
	}

	if len(scan.skippedKeys) > 0 {
		// Asked for again, so "a later catch-up" is a promise rather than a
		// hope: nothing else would bring one along.
		scan.skipped = true
		log.Printf("catch-up read %d conversation threads and skipped %d; the skipped ones are read first on the next pass",
			walked, len(scan.skippedKeys))
	}
	return messages, nil
}

// threadWalkOrder puts the conversations a previous walk could not reach first,
// and is otherwise stable. A map's own order is unspecified, which is how a
// busy conversation could be passed over on every pass while the budget went
// to quiet ones.
func threadWalkOrder(cursors map[threadKey]string, waiting map[threadKey]struct{}) []threadKey {
	keys := make([]threadKey, 0, len(cursors))
	for key := range cursors {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		_, iWaiting := waiting[keys[i]]
		_, jWaiting := waiting[keys[j]]
		if iWaiting != jWaiting {
			return iWaiting
		}
		if keys[i].channel != keys[j].channel {
			return keys[i].channel < keys[j].channel
		}
		return keys[i].threadTS < keys[j].threadTS
	})
	return keys
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
func (b *Bridge) closeThreadLocked(key threadKey) {
	delete(b.threads, key)
	delete(b.threadCursors, key)

	if b.store == nil {
		return
	}
	b.recordStateWriteLocked(stateWrite{
		stateKey: stateKey{kind: writeThread, channel: key.channel, threadTS: key.threadTS},
		remove:   true,
	})
}

// noteMentionCursor records how far the search for mentions has looked.
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
	// The cost of losing this is a repeated scan of a window already read,
	// which the thread cursors then filter out.
	b.recordStateWriteLocked(stateWrite{stateKey: stateKey{kind: writeMentionCursor}, ts: ts})
}
