package bridge

import (
	"context"
	"errors"
	"sort"
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

// absorbReaction folds one emoji from the stream into the pending queue.
//
// It is the reaction half of absorb, and separate for the same reason the
// channel is: what arrives here is never merged with history and never
// compared against a cursor.
func (b *Bridge) absorbReaction(r Reaction) {
	b.mu.Lock()
	defer b.mu.Unlock()

	reaction, ok := b.classifyReactionLocked(r)
	if !ok {
		return
	}
	b.pendingReactions = append(b.pendingReactions, reaction)
	b.notifyPendingLocked()
}

// drainStreamReactions absorbs every reaction already queued on the stream,
// without waiting for more.
//
// It is called on the way out of a dying connection. The socket closes its
// three channels together, and a call that notices the events channel first
// would otherwise abandon whatever emoji were still sitting in the buffer —
// and those are reactions the bridge did receive, which is not the loss the
// live-only limitation describes.
//
// The closed check is not a formality: a closed channel is permanently ready,
// so without it this loop would spin instead of reaching its default. Receiving
// from a closed channel still yields what was buffered first, so nothing that
// arrived before the close is left behind.
func (b *Bridge) drainStreamReactions(reactions <-chan Reaction) {
	for {
		select {
		case r, ok := <-reactions:
			if !ok {
				return
			}
			b.absorbReaction(r)
		default:
			return
		}
	}
}

// drainStreamEvents absorbs every message already queued on the stream, without
// waiting for more.
//
// It runs before a reaction is classified. Messages and reactions arrive on
// channels of their own, and a select picks between two ready channels at
// random, so a reaction on the very message that opens a conversation could
// otherwise be judged before the mention that opened it — and dropped for want
// of a conversation that was already on its way. Socket Mode delivers the
// mention first; this keeps that order where it matters.
func (b *Bridge) drainStreamEvents(stream Stream) error {
	for {
		select {
		case evt, ok := <-stream.Events():
			if !ok {
				return nil
			}
			if err := b.absorb(evt); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// drainReactions takes everything queued for the next delivery.
//
// Reactions have no cursor and no catch-up: they exist only on the live
// connection, so the queue is the whole of what there is to hand over.
func (b *Bridge) drainReactions() []Reaction {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pendingReactions) == 0 {
		return nil
	}
	drained := b.pendingReactions
	b.pendingReactions = nil
	return drained
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
