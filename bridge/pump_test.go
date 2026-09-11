package bridge

import (
	"context"
	"testing"
	"time"
)

// The pump is what makes the socket readable while nobody is calling. Before
// it, an event sat on the channel until some call happened to select on it.
func TestThePumpAbsorbsWithNobodyWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	// One wait to open the connection, and nothing else running afterwards.
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	react(stream, testChannel, "100.000500", colleague, "white_check_mark", true)

	eventually(t, "the pump to take the reaction with no call in flight", func() bool {
		return b.pendingReactionCount() == 1
	})
}

// The ordering guarantee, with a question running: receiving a mention and
// registering the thread it opens are one step now, so a reaction on that
// mention cannot be judged in between and dropped. The scenario is run several
// times because what it is guarding against is a scheduling race.
func TestAMentionAndItsReactionSurviveAConcurrentAsk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for attempt := 0; attempt < 10; attempt++ {
		b, api, stream := mentionBridge(ctx, t)
		api.mu.Lock()
		api.questionTS = askTS
		api.mu.Unlock()

		asked := make(chan AskResult, 1)
		go func() {
			// Interruption off, so the mention below cannot end the question
			// early and change which call hands it over.
			result, err := b.Ask(ctx, AskRequest{
				Question:          "Ship it?",
				Options:           []string{"Yes", "No"},
				Timeout:           MinWaitTimeout,
				InterruptDisabled: true,
			})
			if err != nil {
				t.Errorf("Ask() error = %v", err)
			}
			asked <- result
		}()

		eventually(t, "the question to be posted", func() bool { return b.pendingAskTS() != "" })

		// The mention opens a conversation; the reaction lands on the very
		// message that opened it.
		send(stream, otherChannel, "200.000100", "", mention("ship it?"))
		react(stream, otherChannel, "200.000100", colleague, "white_check_mark", true)

		stream.interactions <- click(testOwner, askTS, 0)

		// A settled question hands over what arrived while it was up, so the
		// mention comes back here; the reaction is slack_wait's to deliver.
		answer := <-asked
		if len(answer.Messages) != 1 {
			t.Fatalf("attempt %d: the question returned %v, want the mention that arrived while it was up", attempt, texts(answer.Messages))
		}

		result := waitOnce(ctx, t, b)
		if len(result.Reactions) != 1 {
			t.Fatalf("attempt %d: delivered %d reactions, want the vote on the message that opened the conversation", attempt, len(result.Reactions))
		}
		if err := b.Close(); err != nil {
			t.Fatalf("attempt %d: Close() error = %v", attempt, err)
		}
	}
}

// A connection that dies while catch-up is fetching history must not take the
// delivery with it. What history returned and what the socket had already
// delivered are both in hand; the disconnection is reported by the call after
// them, not instead of them.
func TestADisconnectDuringCatchUpStillDeliversWhatWasInHand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	gate := make(chan struct{})
	api := &fakeAPI{
		botUserID:   testBotUser,
		historyGate: gate,
		channelHistory: map[string][]candidate{
			testChannel: {
				ownerMsg("100.000100", "already answered"),
				ownerMsg("100.000200", "sent while the socket was dying"),
			},
		},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	done := make(chan WaitResult, 1)
	go func() {
		result, err := b.Wait(ctx, 5*time.Second)
		if err != nil {
			t.Errorf("Wait() error = %v, want the message delivered rather than the disconnection reported", err)
		}
		done <- result
	}()

	// The socket dies while history is in flight.
	eventually(t, "catch-up to reach Slack", func() bool { return len(api.calls()) > 0 })
	close(stream.events)
	close(gate)

	select {
	case result := <-done:
		if len(result.Messages) != 1 || result.Messages[0].TS != "100.000200" {
			t.Fatalf("Wait() returned %v, want the message catch-up was holding", texts(result.Messages))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() never returned")
	}

	// And the disconnection is reported next, rather than swallowed.
	if _, err := b.Wait(ctx, 5*time.Second); err == nil {
		t.Error("Wait() = nil error on a dead connection, want it reported once the batch was handed over")
	}
}

// The ordering rule itself: applying a reaction applies whatever messages are
// already on the wire ahead of it, so the mention that brings a channel into
// scope is registered before the vote on it is judged.
//
// That the two are one step, and not merely adjacent, comes from the single
// lock they share — a call draining the queues waits for both or arrives before
// either, and cannot land between them.
func TestApplyingAReactionAppliesTheMessagesAheadOfIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := mentionBridge(ctx, t)

	// Open the connection, so the bridge knows its own user ID and can
	// recognise the mention below.
	if result := waitOnce(ctx, t, b); !result.TimedOut {
		t.Fatalf("Wait() = %+v, want a timeout on a quiet channel", result)
	}

	// A channel of its own, so what is read here is only what this test put
	// there: the pump has the connection's.
	events := make(chan StreamEvent, 1)
	events <- StreamEvent{Kind: StreamMessage, Message: Message{
		TS: "200.000100", Channel: otherChannel, User: testOwner, Text: mention("ship it?"),
	}}

	b.applyReaction(events, Reaction{
		TS: "200.000100", Channel: otherChannel, User: colleague, Reaction: "white_check_mark", Added: true,
	})

	if kept := b.drainReactions(); len(kept) != 1 {
		t.Fatalf("drainReactions() = %+v, want the vote: the mention ahead of it opens the conversation it is in", kept)
	}
}
