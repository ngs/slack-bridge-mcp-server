package bridge

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

// pendingSubscribers reports how many calls are currently listening for the
// pending queue to grow. A test uses it to know a wait is past its first
// catch-up and actually blocked, rather than sleeping and hoping.
func (b *Bridge) pendingSubscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pendingSubs)
}

// caughtUp reports that the first catch-up has finished. It flips inside the
// same locked section that merges the pending queue, which makes it the point
// after which a message queued by a test cannot be picked up by the drain at
// the top of the wait's loop.
func (b *Bridge) caughtUp() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.needCatchUp
}

// queueWithoutWaking puts a message on the pending queue the way absorb does,
// but without the notification. It stands in for the one ordering a test
// cannot otherwise arrange: a message that lands after the wait has already
// drained and is sitting in its select.
func (b *Bridge) queueWithoutWaking(msg Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = append(b.pending, msg)
}

// Two calls read the same stream, so the one that takes a message off it is
// not necessarily the one waiting for it. A wait blocked in its select cannot
// see the queue grow underneath it, and before the subscription it stayed
// there until its own deadline while the message sat in hand.
func TestWaitWakesWhenAnotherCallAbsorbsAMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)

	type outcome struct {
		result WaitResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := b.Wait(ctx, MaxWaitTimeout)
		done <- outcome{result, err}
	}()

	// Past the first catch-up and subscribed means blocked in the select, which
	// is the state this is about.
	eventually(t, "the wait to finish its catch-up", b.caughtUp)
	eventually(t, "the wait to subscribe", func() bool { return b.pendingSubscribers() > 0 })

	if err := b.absorb(StreamEvent{
		Kind:    StreamMessage,
		Message: Message{TS: "100.000200", User: testOwner, Text: "while you were blocked"},
	}); err != nil {
		t.Fatalf("absorb() error = %v", err)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Wait() error = %v", got.err)
		}
		if want := []string{"while you were blocked"}; !reflect.DeepEqual(texts(got.result.Messages), want) {
			t.Errorf("Wait() messages = %v, want %v", texts(got.result.Messages), want)
		}
		if got.result.TimedOut {
			t.Errorf("Wait() timed_out = true, want the message reported as a delivery")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Wait() did not return; it never noticed the message another call had absorbed")
	}
}

// A deadline is not a reason to throw messages away. Anything that reached the
// queue while the call was blocked is owed to the agent, and returning an empty
// timeout on top of it leaves the owner waiting on a reply that never comes.
func TestWaitDeliversWhatArrivedWhenTheDeadlineFires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, _ := askBridge(ctx, t)

	const timeout = 400 * time.Millisecond

	type outcome struct {
		result  WaitResult
		err     error
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		start := time.Now()
		result, err := b.Wait(ctx, timeout)
		done <- outcome{result, err, time.Since(start)}
	}()

	// Queued only once the catch-up has merged, so the drain at the top of the
	// loop cannot be the thing that finds it, and queued without waking the
	// call so nothing but the deadline can end it.
	eventually(t, "the wait to finish its catch-up", b.caughtUp)
	eventually(t, "the wait to subscribe", func() bool { return b.pendingSubscribers() > 0 })
	b.queueWithoutWaking(Message{TS: "100.000200", User: testOwner, Text: "just before the bell"})

	got := <-done
	if got.err != nil {
		t.Fatalf("Wait() error = %v", got.err)
	}
	if got.elapsed < timeout/2 {
		t.Fatalf("Wait() returned after %s, well inside its %s timeout; this did not exercise the deadline", got.elapsed, timeout)
	}
	if want := []string{"just before the bell"}; !reflect.DeepEqual(texts(got.result.Messages), want) {
		t.Errorf("Wait() messages = %v, want %v", texts(got.result.Messages), want)
	}
	if got.result.TimedOut {
		t.Errorf("Wait() timed_out = true with messages to hand over, want it reported as a delivery")
	}
}

// The owner changing their mind is an answer of a kind. A question that stays
// on the channel after they have moved on leaves the agent blocked on a click
// that is never coming, so a message takes the question down and comes back
// instead of it.
func TestAskIsInterruptedByAMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := askBridge(ctx, t)

	stream.events <- StreamEvent{
		Kind:    StreamMessage,
		Message: Message{TS: "100.000200", User: testOwner, Text: "never mind, do this instead"},
	}

	result, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if !result.Interrupted {
		t.Errorf("Ask() interrupted = false, want the message reported as an interruption")
	}
	if result.TimedOut {
		t.Errorf("Ask() timed_out = true, want an interruption told apart from a timeout")
	}
	if result.ChoiceIndex != -1 {
		t.Errorf("Ask() choice_index = %d, want -1; nothing was clicked", result.ChoiceIndex)
	}
	if want := []string{"never mind, do this instead"}; !reflect.DeepEqual(texts(result.Messages), want) {
		t.Errorf("Ask() messages = %v, want %v", texts(result.Messages), want)
	}

	// The buttons have to go with it: they answer a question the agent has
	// stopped waiting on.
	resolutions := api.snapshotResolutions()
	if len(resolutions) != 1 {
		t.Fatalf("Ask() resolved the question %d times, want 1", len(resolutions))
	}
	if !strings.Contains(resolutions[0].Text, "superseded") || !strings.Contains(resolutions[0].Text, "Deploy now?") {
		t.Errorf("resolution text = %q, want the question marked superseded", resolutions[0].Text)
	}
}

// Opting out puts the old behaviour back: the question waits for its click, and
// the message stays queued for whoever asks next.
func TestAskWithoutInterruptionWaitsForTheClick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	stream.events <- StreamEvent{
		Kind:    StreamMessage,
		Message: Message{TS: "100.000200", User: testOwner, Text: "one more thing"},
	}
	stream.interactions <- click(testOwner, askTS, 1)

	result, err := b.Ask(ctx, AskRequest{
		Question:          "Deploy now?",
		Options:           []string{"Yes", "No"},
		Timeout:           MaxWaitTimeout,
		InterruptDisabled: true,
	})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}
	if result.Interrupted {
		t.Errorf("Ask() interrupted = true with interruption turned off, want the click awaited")
	}
	if result.ChoiceIndex != 1 {
		t.Errorf("Ask() choice_index = %d, want the click at 1", result.ChoiceIndex)
	}

	next, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if want := []string{"one more thing"}; !reflect.DeepEqual(texts(next.Messages), want) {
		t.Errorf("Wait() messages = %v, want the message kept for the next wait", texts(next.Messages))
	}
}

// However the message comes back — handed over by the question it interrupted,
// or by the wait that follows — it must come back once. Delivering it twice
// makes the agent answer the owner twice; delivering it not at all loses it.
func TestAMessageDuringAQuestionIsDeliveredExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := askBridge(ctx, t)

	stream.events <- StreamEvent{
		Kind:    StreamMessage,
		Message: Message{TS: "100.000300", User: testOwner, Text: "one more thing"},
	}
	stream.interactions <- click(testOwner, askTS, 0)

	asked, err := b.Ask(ctx, AskRequest{Question: "Deploy now?", Options: []string{"Yes", "No"}, Timeout: MaxWaitTimeout})
	if err != nil {
		t.Fatalf("Ask() error = %v", err)
	}

	waited, err := b.Wait(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	delivered := 0
	for _, text := range append(texts(asked.Messages), texts(waited.Messages)...) {
		if text == "one more thing" {
			delivered++
		}
	}
	if delivered != 1 {
		t.Errorf("the message was delivered %d times across the question and the wait, want exactly 1", delivered)
	}
}
