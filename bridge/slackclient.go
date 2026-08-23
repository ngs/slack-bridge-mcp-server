package bridge

import (
	"context"
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
		events:  make(chan StreamEvent, liveEventBuffer),
		channel: cfg.Channel,
		owner:   cfg.Owner,
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
		Cursor:    req.Cursor,
		Limit:     req.Limit,
	})
	if err != nil {
		return HistoryPage{}, fmt.Errorf("conversations.history: %w", err)
	}

	page := HistoryPage{
		Messages:   make([]candidate, 0, len(resp.Messages)),
		NextCursor: resp.ResponseMetaData.NextCursor,
	}
	for _, m := range resp.Messages {
		page.Messages = append(page.Messages, candidate{
			// history results are scoped to the requested channel, and
			// Slack does not echo it back on each message.
			Channel:  req.Channel,
			User:     m.User,
			BotID:    m.BotID,
			SubType:  m.SubType,
			Text:     m.Text,
			TS:       m.Timestamp,
			ThreadTS: m.ThreadTimestamp,
		})
	}
	return page, nil
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
	if err := w.client.AddReactionContext(ctx, emoji, slack.NewRefToMessage(channel, ts)); err != nil {
		return fmt.Errorf("reactions.add: %w", err)
	}
	return nil
}

// socketModeStream translates socketmode events into StreamEvents, applying
// the owner filter before anything is queued.
type socketModeStream struct {
	events  chan StreamEvent
	channel string
	owner   string
	// dropped is set when an event could not be queued. It is sticky rather
	// than an event of its own because the queue being full is exactly when
	// a StreamDropped event would not fit either; the flag is converted into
	// one as soon as there is room.
	dropped atomic.Bool
}

func (s *socketModeStream) Events() <-chan StreamEvent { return s.events }

func (s *socketModeStream) consume(ctx context.Context, client *socketmode.Client) {
	defer close(s.events)

	for {
		s.flushDropped()

		select {
		case <-ctx.Done():
			return
		case evt, ok := <-client.Events:
			if !ok {
				return
			}
			s.handle(client, evt)
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

func (s *socketModeStream) handle(client *socketmode.Client, evt socketmode.Event) {
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
			_ = client.Ack(*evt.Request)
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

	case socketmode.EventTypeInvalidAuth:
		log.Printf("slack rejected the app-level token; check %s", EnvAppToken)

	default:
		// Slash commands, interactive payloads and the connection
		// lifecycle chatter are not part of the bridge.
		if evt.Request != nil && evt.Type != socketmode.EventTypeHello {
			_ = client.Ack(*evt.Request)
		}
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
