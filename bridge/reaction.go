package bridge

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
)

// Reaction is one emoji reaction, added or removed, as slack_wait hands it
// over.
//
// It is not a message and does not pretend to be one: it carries no text, it
// is never acknowledged with a receipt, and it does not start the processing
// indicator. What it says is that somebody put an emoji on a message the
// session can see, which is how an agent collecting votes on a decision
// learns that a vote was cast.
type Reaction struct {
	// TS identifies the message that was reacted to, not the reaction itself.
	// It is the ts to pass to slack_reactions, slack_post or slack_ack.
	TS string `json:"ts"`
	// Channel is where that message lives, and is always set: a ts means
	// nothing without it.
	Channel string `json:"channel"`
	// User is the Slack user ID of whoever reacted. Unlike a message, this is
	// not necessarily the owner — an approval needs everybody else's emoji too.
	User string `json:"user"`
	// UserName is the name a person would recognise, resolved the way
	// slack_history resolves a message author. It falls back to the raw ID.
	UserName string `json:"user_name"`
	// Reaction is the emoji name without colons, as Slack sends it.
	Reaction string `json:"reaction"`
	// Added distinguishes reaction_added from reaction_removed: true when the
	// emoji went on, false when it came off.
	Added bool `json:"added"`
	// EventTS is when Slack recorded the reaction. It orders reactions among
	// themselves; TS orders the messages they are on, and the two are
	// unrelated.
	EventTS string `json:"event_ts,omitempty"`
}

// classifyReactionLocked decides whether a reaction belongs to a conversation
// the session is having. The caller must hold b.mu.
//
// The scope follows the messages: the home channel, and any channel where a
// mention has opened a conversation. Elsewhere the app is a guest, and
// relaying a colleague's emoji from a channel nobody opened would put that
// channel's traffic in the agent's context exactly as relaying its messages
// would.
//
// Two differences from the message rules are deliberate. Any user counts,
// because a reaction is how other people answer a decision candidate, and one
// that only relayed the owner would be useless for that. And outside the home
// channel the test is the channel rather than the thread: a reaction event
// carries the reacted message's ts and nothing else, so whether that message
// sits inside an open thread cannot be known without another API call on the
// socket's path. A channel with a conversation open in it is the closest
// answer that costs nothing, and it errs towards delivering.
//
// The bridge's own reactions are dropped. The automatic 👀 receipt goes on
// every delivered message, and feeding those back would answer each message
// with an event about itself.
func (b *Bridge) classifyReactionLocked(r Reaction) (Reaction, bool) {
	if r.TS == "" || r.Reaction == "" {
		return Reaction{}, false
	}
	if r.User != "" && r.User == b.botUserID {
		return Reaction{}, false
	}
	if r.Channel == "" || r.Channel == b.cfg.Channel {
		if r.Channel == "" {
			r.Channel = b.cfg.Channel
		}
		return r, true
	}
	if b.channelHasConversationLocked(r.Channel) {
		return r, true
	}
	return Reaction{}, false
}

// channelHasConversationLocked reports whether any conversation is open in a
// channel. The caller must hold b.mu.
func (b *Bridge) channelHasConversationLocked(channel string) bool {
	for key, open := range b.threads {
		if open && key.channel == channel {
			return true
		}
	}
	return false
}

// absorbReactionLocked folds one emoji from the stream into the pending queue.
// The caller must hold b.mu, which is what lets a reaction and the messages
// ahead of it be applied as one step.
//
// It queues without judging. Whether a reaction belongs to a conversation the
// session is in is decided when the batch is handed over, by which time
// catch-up has run and every message in the batch has been absorbed — so a
// reaction cannot be dropped for want of a mention that was moments behind it,
// wherever that mention happened to be at this instant: still on the socket,
// taken by a concurrent call, or waiting in history to be recovered on a
// reconnect. The one thing decided here is whether this is a reaction the
// bridge has already seen.
//
// The dedup lives on the bridge rather than on the stream because a connection
// is replaced on every reconnect, and Slack redelivers what it was not
// acknowledged for — possibly on the replacement. A window on the stream would
// be empty exactly when the redelivery arrived.
func (b *Bridge) absorbReactionLocked(r Reaction) {
	if r.TS == "" || r.Reaction == "" || b.seenReactionLocked(r) {
		return
	}
	if len(b.pendingReactions) >= maxPendingReactions {
		// The agent has not called back in long enough for this many emoji to
		// pile up. Dropping the oldest keeps the newest votes, and the marker
		// is what tells the agent to stop trusting its count.
		b.pendingReactions = b.pendingReactions[1:]
		b.reactionsDropped = true
	}
	b.pendingReactions = append(b.pendingReactions, r)
	b.notifyPendingLocked()
}

// maxPendingReactions bounds the queue of emoji waiting to be handed over. It
// is generous — a decision post collects a burst, not a stream — and exists so
// that a busy workspace cannot grow the queue without limit while nothing is
// calling slack_wait.
const maxPendingReactions = 512

// reactionDedupWindow is how many recent reactions are remembered so a
// redelivered envelope is not counted twice. Slack redelivers anything it is
// not acknowledged for, and an acknowledgement can fail: it goes out before the
// event is handled, and nothing downstream hears whether it landed. Messages
// survive that because history merges them by timestamp; a reaction has no
// history and no merge, so a consumer counting votes would count one twice.
const reactionDedupWindow = 1024

// seenReactionLocked reports whether this exact reaction has been queued
// before, and records it if not. The caller must hold b.mu.
//
// The key is the whole event: who reacted, with what, to which message, when,
// and whether it went on or came off. A redelivery repeats all of it, while two
// genuinely different actions differ in at least the timestamp — so this drops
// duplicates without ever swallowing a vote somebody actually cast.
func (b *Bridge) seenReactionLocked(r Reaction) bool {
	key := strings.Join([]string{r.User, r.Reaction, r.Channel, r.TS, r.EventTS, strconv.FormatBool(r.Added)}, "\x00")

	if _, ok := b.seenReactions[key]; ok {
		return true
	}
	if b.seenReactions == nil {
		b.seenReactions = make(map[string]struct{}, reactionDedupWindow)
	}
	b.seenReactions[key] = struct{}{}
	b.seenReactionOrder = append(b.seenReactionOrder, key)
	if len(b.seenReactionOrder) > reactionDedupWindow {
		delete(b.seenReactions, b.seenReactionOrder[0])
		b.seenReactionOrder = b.seenReactionOrder[1:]
	}
	return false
}

// noteReactionsDropped records that a reaction was received and will not be
// delivered, so the next result tells the agent to read the tally back.
func (b *Bridge) noteReactionsDropped() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.noteReactionsDroppedLocked()
}

// noteReactionsDroppedLocked is noteReactionsDropped for a caller that already
// holds b.mu, which the pump does for everything it applies.
func (b *Bridge) noteReactionsDroppedLocked() {
	b.reactionsDropped = true
	// A loss is something to hear about as much as a reaction is: a wait
	// blocked on a long timeout would otherwise sit out the whole of it before
	// telling the agent its count is wrong.
	b.notifyPendingLocked()
}

// drainReactions takes everything queued for the next delivery.
//
// Reactions have no cursor and no catch-up: they exist only on the live
// connection, so the queue is the whole of what there is to hand over.
func (b *Bridge) drainReactions(generation uint64) []Reaction {
	b.mu.Lock()
	defer b.mu.Unlock()

	// The queue belongs to whichever connection is current, and a call on one
	// that has been replaced would be taking the replacement's reactions —
	// which the call that wants them would then never see.
	if b.stale(generation) {
		return nil
	}
	return b.drainReactionsLocked()
}

// drainReactionsLocked is drainReactions for a caller that already holds b.mu
// and has checked its generation — which is how a batch of messages and the
// reactions that arrived with them leave the queues as one step.
func (b *Bridge) drainReactionsLocked() []Reaction {

	if len(b.pendingReactions) == 0 {
		return nil
	}
	queued := b.pendingReactions
	b.pendingReactions = nil

	// Judged here, against the conversations that are open now. A reaction is
	// delivered when the message it is on belongs to a conversation the session
	// is in by the time the batch goes out — not by the order two channels
	// happened to be read in.
	//
	// A reaction that matches nothing is dropped here and not held. It was
	// held while two goroutines read the socket, to cover the instant between
	// one of them receiving a mention and registering the thread it opens; the
	// pump closed that instant, and a reaction still matching nothing is one
	// Slack genuinely sent before the agent was part of the conversation. Those
	// are a tally rather than an event, and slack_reactions reads tallies.
	kept := make([]Reaction, 0, len(queued))
	for _, r := range queued {
		if reaction, ok := b.classifyReactionLocked(r); ok {
			kept = append(kept, reaction)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// takeReactionsDropped reports whether any reaction has been lost since the
// agent was last told, and clears the record.
//
// It reads the live stream's marker and the bridge's own together: a loss on a
// connection that has since died is still a loss the agent has to hear about,
// and the stream it happened on is gone.
func (b *Bridge) takeReactionsDropped(generation uint64) bool {
	b.mu.Lock()
	stream := b.stream
	current := !b.stale(generation)
	b.mu.Unlock()

	// Outside the lock, because it is a question for somebody else's
	// implementation of the stream and not for the bridge.
	dropped := current && streamDroppedReactions(stream)

	b.mu.Lock()
	defer b.mu.Unlock()

	// A call on a connection since replaced is about to be told so, and reports
	// nothing. Clearing the marker here would spend it on that call and leave
	// the next live wait saying a loss never happened — including the one just
	// taken off the stream, which is why it is put back rather than dropped.
	if b.stale(generation) {
		if dropped {
			b.reactionsDropped = true
		}
		return false
	}

	dropped = dropped || b.reactionsDropped
	b.reactionsDropped = false
	return dropped
}

// nameReactions fills in the display name on each reaction, leaving the ID in
// place when it cannot be resolved.
//
// It runs at delivery rather than on the socket so a users.info round trip
// never sits between Slack and the queue, and it shares slack_history's cache,
// so the people who react to things are looked up once per session.
func (b *Bridge) nameReactions(ctx context.Context, reactions []Reaction) []Reaction {
	if len(reactions) == 0 {
		return reactions
	}

	api := b.currentAPI()
	if api == nil {
		return reactions
	}

	names := b.resolveNames(ctx, api, reactionAuthors(reactions))
	for i := range reactions {
		if name := names[reactions[i].User]; name != "" {
			reactions[i].UserName = name
		} else {
			reactions[i].UserName = reactions[i].User
		}
	}
	return reactions
}

// reactionAuthors turns reactions into the shape resolveNames reads, so the
// batching and the failure rule are shared with slack_history rather than
// written twice.
func reactionAuthors(reactions []Reaction) []candidate {
	out := make([]candidate, 0, len(reactions))
	for _, r := range reactions {
		out = append(out, candidate{User: r.User})
	}
	return out
}

// currentAPI returns the connected Web API client, or nil if the bridge never
// connected. It exists so a call can reach Slack without holding b.mu across
// the request.
func (b *Bridge) currentAPI() API {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.api
}

// ReactionsRequest asks for the reactions on one message.
type ReactionsRequest struct {
	// TS is the message to read. Required: there is no default message.
	TS string
	// Channel is where it lives. Empty means the home channel.
	Channel string
}

// ReactionSummary is one emoji on a message and who put it there.
type ReactionSummary struct {
	Name  string         `json:"name"`
	Count int            `json:"count"`
	Users []ReactionUser `json:"users"`
}

// ReactionUser is one person who reacted, named as slack_history names an
// author.
type ReactionUser struct {
	ID       string `json:"id"`
	UserName string `json:"user_name"`
}

// ReactionsResult is what slack_reactions returns.
type ReactionsResult struct {
	Reactions []ReactionSummary `json:"reactions"`
}

// Reactions reports the emoji currently on a message.
//
// It is the counterpart to the reactions slack_wait delivers, and exists
// because those are live-only: a reaction added while the session was down is
// in no history and no catch-up, so the standing tally has to be asked for.
// Like slack_history it changes nothing — it moves no cursor and consumes
// nothing a wait would deliver.
func (b *Bridge) Reactions(ctx context.Context, req ReactionsRequest) (ReactionsResult, error) {
	// Before connecting: a ts is the whole of what identifies the message, and
	// answering an empty one with a deterministic error beats opening a socket
	// to have Slack reject the reference.
	if req.TS == "" {
		return ReactionsResult{}, errors.New("ts is required")
	}

	api, _, err := b.apiForCall()
	if err != nil {
		return ReactionsResult{}, err
	}
	reader, ok := api.(ReactionReader)
	if !ok {
		return ReactionsResult{}, errors.New("this Slack connection cannot read reactions")
	}

	summaries, err := reader.MessageReactions(ctx, b.channelFor(req.Channel), req.TS)
	if err != nil {
		return ReactionsResult{}, err
	}

	names := b.resolveNames(ctx, api, reactionUserCandidates(summaries))
	for i := range summaries {
		for j := range summaries[i].Users {
			id := summaries[i].Users[j].ID
			if name := names[id]; name != "" {
				summaries[i].Users[j].UserName = name
			} else {
				summaries[i].Users[j].UserName = id
			}
		}
	}

	// Slack returns them in the order they were first used, which is worth
	// keeping; the users within one emoji are sorted so two reads of an
	// unchanged message look the same.
	for i := range summaries {
		sort.SliceStable(summaries[i].Users, func(a, c int) bool {
			return summaries[i].Users[a].ID < summaries[i].Users[c].ID
		})
	}

	return ReactionsResult{Reactions: summaries}, nil
}

func reactionUserCandidates(summaries []ReactionSummary) []candidate {
	var out []candidate
	for _, s := range summaries {
		for _, u := range s.Users {
			out = append(out, candidate{User: u.ID})
		}
	}
	return out
}
