package bridge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

// liveEventBuffer is how many live messages may queue up while no slack_wait
// is in flight. Overflow is not data loss: the stream reports StreamDropped
// and the bridge recovers the messages from conversations.history.
const liveEventBuffer = 64

// liveInteractionBuffer is the click queue. It is small because clicks are
// rare — at most one question is outstanding at a time — and separate because
// a click that does not fit is simply lost: unlike a message, there is no
// history to recover it from.
const liveInteractionBuffer = 16

// SocketModeConnector is the production Connector: a Web API client for
// history, posting and reactions, plus a Socket Mode WebSocket for live
// events.
type SocketModeConnector struct{}

// Connect authenticates, starts the Socket Mode client in a goroutine and
// returns the two halves of the connection. The goroutine runs until ctx is
// cancelled, which happens when the MCP server shuts down with the session.
func (SocketModeConnector) Connect(ctx context.Context, cfg Config) (API, Stream, error) {
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}

	api := slack.New(cfg.BotToken, slack.OptionAppLevelToken(cfg.AppToken))

	// Fail here rather than on the first tool call, so a bad token is
	// reported as "cannot connect" instead of a confusing empty history.
	if _, err := api.AuthTestContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("authenticating with Slack: %w", err)
	}

	client := socketmode.New(api)
	stream := &socketModeStream{
		events:       make(chan StreamEvent, liveEventBuffer),
		interactions: make(chan Interaction, liveInteractionBuffer),
		channel:      cfg.Channel,
		owner:        cfg.Owner,
	}

	go stream.consume(ctx, client)
	go func() {
		// RunContext reconnects on its own; it only returns when ctx is
		// cancelled or the connection fails unrecoverably. It never returns a
		// nil error, so only the "stopped without cancellation" case is worth
		// reporting.
		err := client.RunContext(ctx)
		if ctx.Err() == nil {
			log.Printf("socket mode client stopped: %v", err)
		}
	}()

	return &webAPI{client: api}, stream, nil
}

// webAPI adapts *slack.Client to the API interface.
type webAPI struct {
	client *slack.Client
}

func (w *webAPI) History(ctx context.Context, req HistoryRequest) (HistoryPage, error) {
	resp, err := w.client.GetConversationHistoryContext(ctx, &slack.GetConversationHistoryParameters{
		ChannelID: req.Channel,
		Oldest:    req.Oldest,
		Latest:    req.Latest,
		Cursor:    req.Cursor,
		Limit:     req.Limit,
	})
	if err != nil {
		return HistoryPage{}, fmt.Errorf("conversations.history: %w", err)
	}

	page := HistoryPage{
		Messages:   make([]candidate, 0, len(resp.Messages)),
		NextCursor: resp.ResponseMetaData.NextCursor,
		HasMore:    resp.HasMore,
	}
	for _, m := range resp.Messages {
		page.Messages = append(page.Messages, toCandidate(req.Channel, m))
	}
	return page, nil
}

func (w *webAPI) Replies(ctx context.Context, req RepliesRequest) (HistoryPage, error) {
	msgs, hasMore, nextCursor, err := w.client.GetConversationRepliesContext(ctx, &slack.GetConversationRepliesParameters{
		ChannelID: req.Channel,
		Timestamp: req.ThreadTS,
		Oldest:    req.Oldest,
		Latest:    req.Latest,
		Cursor:    req.Cursor,
		Limit:     req.Limit,
	})
	if err != nil {
		// A thread that is not there to be read is a different kind of
		// problem from a thread Slack would not give us this time, and
		// catch-up has to tell them apart: one is skipped, the other is
		// retried.
		var slackErr slack.SlackErrorResponse
		if errors.As(err, &slackErr) && permanentThreadErrors[slackErr.Err] {
			return HistoryPage{}, fmt.Errorf("%w: %s", ErrThreadUnreadable, slackErr.Err)
		}
		return HistoryPage{}, fmt.Errorf("conversations.replies: %w", err)
	}

	page := HistoryPage{
		Messages:   make([]candidate, 0, len(msgs)),
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}
	for _, m := range msgs {
		page.Messages = append(page.Messages, toCandidate(req.Channel, m))
	}
	return page, nil
}

// UserName prefers the name the person chose to show over their real name,
// and falls back to the ID so a caller always has something to print.
func (w *webAPI) UserName(ctx context.Context, userID string) (string, error) {
	user, err := w.client.GetUserInfoContext(ctx, userID)
	if err != nil {
		return "", fmt.Errorf("users.info: %w", err)
	}

	for _, name := range []string{user.Profile.DisplayName, user.Profile.RealName, user.RealName, user.Name} {
		if name != "" {
			return name, nil
		}
	}
	return userID, nil
}

// toCandidate normalises a Web API message. The channel is passed in because
// results are scoped to the channel that was asked for, and Slack does not
// echo it back on each message.
func toCandidate(channel string, m slack.Message) candidate {
	// An app post carries its display name in one of two places depending on
	// how it was sent: the top-level username for incoming webhooks and older
	// bot messages, the bot profile for apps posting as themselves. Without
	// the second, slack_history shows those posts as a raw bot ID.
	username := m.Username
	if username == "" && m.BotProfile != nil {
		username = m.BotProfile.Name
	}

	return candidate{
		Channel:     channel,
		User:        m.User,
		BotID:       m.BotID,
		SubType:     m.SubType,
		Text:        m.Text,
		TS:          m.Timestamp,
		ThreadTS:    m.ThreadTimestamp,
		Username:    username,
		ReplyCount:  m.ReplyCount,
		LatestReply: m.LatestReply,
	}
}

func (w *webAPI) Post(ctx context.Context, channel, threadTS, text string) (string, error) {
	options := []slack.MsgOption{slack.MsgOptionText(text, false)}
	if threadTS != "" {
		options = append(options, slack.MsgOptionTS(threadTS))
	}

	_, ts, err := w.client.PostMessageContext(ctx, channel, options...)
	if err != nil {
		return "", fmt.Errorf("chat.postMessage: %w", err)
	}
	return ts, nil
}

func (w *webAPI) PostQuestion(ctx context.Context, channel, threadTS string, q Question) (string, error) {
	buttons := make([]slack.BlockElement, 0, len(q.Options))
	for _, opt := range q.Options {
		buttons = append(buttons, slack.NewButtonBlockElement(
			opt.ActionID,
			opt.Value,
			// Button labels are plain_text; mrkdwn is not rendered there.
			slack.NewTextBlockObject(slack.PlainTextType, opt.Label, false, false),
		))
	}

	options := []slack.MsgOption{
		// The text is the notification fallback, which is what the owner's
		// phone shows on the lock screen; the section block is the message.
		slack.MsgOptionText(q.Text, false),
		slack.MsgOptionBlocks(
			slack.NewSectionBlock(slack.NewTextBlockObject(slack.MarkdownType, q.Text, false, false), nil, nil),
			slack.NewActionBlock(q.BlockID, buttons...),
		),
	}
	if threadTS != "" {
		options = append(options, slack.MsgOptionTS(threadTS))
	}

	_, ts, err := w.client.PostMessageContext(ctx, channel, options...)
	if err != nil {
		return "", fmt.Errorf("chat.postMessage: %w", err)
	}
	return ts, nil
}

// ResolveQuestion rewrites the question as plain text. The empty
// MsgOptionBlocks is the part that matters: chat.update leaves the existing
// blocks in place unless it is sent an explicit empty list, and blocks left
// behind are buttons the owner can still click.
func (w *webAPI) ResolveQuestion(ctx context.Context, channel, ts, text string) error {
	if _, _, _, err := w.client.UpdateMessageContext(ctx, channel, ts,
		slack.MsgOptionText(text, false),
		slack.MsgOptionBlocks(),
	); err != nil {
		return fmt.Errorf("chat.update: %w", err)
	}
	return nil
}

func (w *webAPI) Update(ctx context.Context, channel, ts, text string) error {
	if _, _, _, err := w.client.UpdateMessageContext(ctx, channel, ts, slack.MsgOptionText(text, false)); err != nil {
		return fmt.Errorf("chat.update: %w", err)
	}
	return nil
}

func (w *webAPI) Delete(ctx context.Context, channel, ts string) error {
	if _, _, err := w.client.DeleteMessageContext(ctx, channel, ts); err != nil {
		return fmt.Errorf("chat.delete: %w", err)
	}
	return nil
}

func (w *webAPI) React(ctx context.Context, channel, ts, emoji string) error {
	err := w.client.AddReactionContext(ctx, emoji, slack.NewRefToMessage(channel, ts))
	if err == nil {
		return nil
	}

	// The emoji already being on the message is the outcome the caller wanted,
	// and it is routine: the automatic receipt reaction and an explicit
	// slack_ack can easily pick the same emoji for the same message.
	var slackErr slack.SlackErrorResponse
	if errors.As(err, &slackErr) && slackErr.Err == "already_reacted" {
		return ErrAlreadyReacted
	}
	return fmt.Errorf("reactions.add: %w", err)
}

// permanentThreadErrors are the conversations.replies failures that will still
// be failures on the next attempt: the thread, its parent, or the channel is
// not there. Everything else — rate limits, timeouts, a Slack outage — is
// worth another go.
var permanentThreadErrors = map[string]bool{
	"thread_not_found":  true,
	"message_not_found": true,
	"channel_not_found": true,
	"not_in_channel":    true,
}

// socketModeStream translates socketmode events into StreamEvents, applying
// the owner filter before anything is queued.
type socketModeStream struct {
	events       chan StreamEvent
	interactions chan Interaction
	channel      string
	owner        string
	// dropped is set when an event could not be queued. It is sticky rather
	// than an event of its own because the queue being full is exactly when
	// a StreamDropped event would not fit either; the flag is converted into
	// one as soon as there is room.
	dropped atomic.Bool
}

func (s *socketModeStream) Events() <-chan StreamEvent { return s.events }

func (s *socketModeStream) Interactions() <-chan Interaction { return s.interactions }

func (s *socketModeStream) consume(ctx context.Context, client *socketmode.Client) {
	defer close(s.events)
	defer close(s.interactions)

	ack := func(req socketmode.Request) { _ = client.Ack(req) }

	for {
		s.flushDropped()

		select {
		case <-ctx.Done():
			return
		case evt, ok := <-client.Events:
			if !ok {
				return
			}
			s.handle(ack, evt)
		}
	}
}

// flushDropped turns a recorded overflow into a StreamDropped event once the
// consumer has made room, so the bridge still learns it needs to catch up.
func (s *socketModeStream) flushDropped() {
	if !s.dropped.Load() {
		return
	}
	select {
	case s.events <- StreamEvent{Kind: StreamDropped}:
		s.dropped.Store(false)
	default:
	}
}

// acker acknowledges one Socket Mode envelope. It is a function rather than
// the client itself so the translation below can be tested without a
// WebSocket, which is the whole of what handle needs the client for.
type acker func(req socketmode.Request)

func (s *socketModeStream) handle(ack acker, evt socketmode.Event) {
	switch evt.Type {
	case socketmode.EventTypeConnected:
		s.emit(StreamEvent{Kind: StreamConnected})

	case socketmode.EventTypeEventsAPI:
		api, ok := evt.Data.(slackevents.EventsAPIEvent)
		if !ok {
			return
		}
		// Slack retries anything it is not acknowledged for. Acknowledge
		// every events_api envelope, including those the owner filter
		// discards, so unrelated channel traffic is not redelivered.
		if evt.Request != nil {
			ack(*evt.Request)
		}

		inner, ok := api.InnerEvent.Data.(*slackevents.MessageEvent)
		if !ok {
			return
		}
		msg, ok := accept(candidate{
			Channel:  inner.Channel,
			User:     inner.User,
			BotID:    inner.BotID,
			SubType:  inner.SubType,
			Text:     inner.Text,
			TS:       inner.TimeStamp,
			ThreadTS: inner.ThreadTimeStamp,
		}, s.channel, s.owner)
		if !ok {
			return
		}
		s.emit(StreamEvent{Kind: StreamMessage, Message: msg})

	case socketmode.EventTypeInteractive:
		// Acknowledge first and unconditionally, exactly as for events_api:
		// Slack redelivers anything it is not acknowledged for, and a click
		// this bridge has no use for would otherwise come back repeatedly.
		if evt.Request != nil {
			ack(*evt.Request)
		}

		cb, ok := evt.Data.(slack.InteractionCallback)
		if !ok || cb.Type != slack.InteractionTypeBlockActions {
			return
		}
		if len(cb.ActionCallback.BlockActions) == 0 {
			return
		}
		// A question has one button per option, so a click is one action.
		action := cb.ActionCallback.BlockActions[0]

		// Container carries the message the button lives on for block_actions;
		// Message is populated too, but only the container is guaranteed.
		channel := cb.Container.ChannelID
		if channel == "" {
			channel = cb.Channel.ID
		}
		messageTS := cb.Container.MessageTs
		if messageTS == "" {
			messageTS = cb.Message.Timestamp
		}

		s.emitInteraction(Interaction{
			User:      cb.User.ID,
			Channel:   channel,
			MessageTS: messageTS,
			BlockID:   action.BlockID,
			ActionID:  action.ActionID,
			Value:     action.Value,
		})

	case socketmode.EventTypeInvalidAuth:
		log.Printf("slack rejected the app-level token; check %s", EnvAppToken)

	default:
		// Slash commands, view submissions and the connection lifecycle
		// chatter are not part of the bridge.
		if evt.Request != nil && evt.Type != socketmode.EventTypeHello {
			ack(*evt.Request)
		}
	}
}

// emitInteraction queues a click on its own channel. There is no fallback if
// it does not fit: a click exists nowhere but this connection, so the loss is
// logged plainly rather than dressed up as a catch-up.
func (s *socketModeStream) emitInteraction(in Interaction) {
	select {
	case s.interactions <- in:
	default:
		log.Printf("dropped a button click because nothing was reading them; the question it answered will time out")
	}
}

// emit queues an event, recording an overflow rather than blocking the
// socketmode reader when nothing is consuming. A blocked reader would stall
// acknowledgements and make Slack redeliver, which is worse than a catch-up.
//
// Nothing is lost either way: the recorded overflow becomes a StreamDropped
// event as soon as the queue drains, and the bridge answers that by re-reading
// the window from conversations.history.
func (s *socketModeStream) emit(evt StreamEvent) {
	select {
	case s.events <- evt:
	default:
		log.Printf("live event buffer is full; falling back to history catch-up")
		s.dropped.Store(true)
	}
}
