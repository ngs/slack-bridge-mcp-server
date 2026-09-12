package bridge

import (
	"testing"
	"time"
)

func newTestStream(size int) *socketModeStream {
	return &socketModeStream{
		events:       make(chan StreamEvent, size),
		interactions: make(chan Interaction, size),
		reactions:    make(chan Reaction, size),
		owner:        testOwner,
	}
}

// A full queue means the agent has been busy while the owner kept typing. The
// overflow must not be silently forgotten: it has to come back as a
// StreamDropped once there is room, which is what sends the bridge to
// conversations.history for the messages it could not queue.
func TestOverflowIsRecoveredAsADropMarker(t *testing.T) {
	stream := newTestStream(2)

	stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000100"}})
	stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000200"}})
	// The queue is full; this one cannot be delivered live.
	stream.emit(StreamEvent{Kind: StreamMessage, Message: Message{TS: "100.000300"}})

	if !stream.dropped.Load() {
		t.Fatal("the overflow was not recorded")
	}

	// No room yet, so there is nothing to flush into.
	stream.flushDropped()
	if len(stream.events) != 2 {
		t.Fatalf("queue length = %d after flushing into a full queue, want 2", len(stream.events))
	}
	if !stream.dropped.Load() {
		t.Error("the overflow flag was cleared without a StreamDropped being queued")
	}

	// The consumer takes one, which is the reader loop's cue to flush.
	<-stream.events
	stream.flushDropped()

	if stream.dropped.Load() {
		t.Error("the overflow flag is still set after a successful flush")
	}

	<-stream.events // the second message
	evt := <-stream.events
	if evt.Kind != StreamDropped {
		t.Errorf("queued event kind = %v, want StreamDropped", evt.Kind)
	}
}

func TestFlushDroppedIsANoOpWhenNothingWasDropped(t *testing.T) {
	stream := newTestStream(4)

	stream.flushDropped()

	if len(stream.events) != 0 {
		t.Errorf("flushDropped queued %d events with no overflow recorded, want 0", len(stream.events))
	}
}

// The flag that says a message was refused is cleared before the announcement
// goes out, not after. Between a successful send and a later clear the bridge
// sees both — it asks the flag as well as reading the announcement — so one
// refusal is counted twice, and the flag left standing then swallows the next
// announcement whole.
func TestTheOverflowFlagIsClearedBeforeItIsAnnounced(t *testing.T) {
	// Unbuffered, so whoever takes the announcement is holding it at the
	// moment it asks — which is the moment the bridge's pump is in when it
	// absorbs the event and polls the flag on the same turn.
	s := &socketModeStream{events: make(chan StreamEvent)}
	s.dropped.Store(true)

	// The interleaving this guards against cannot be forced from a test: with
	// the clear after the send, the sender usually gets to it before the
	// receiver asks. What is pinned here is the invariant either way — once
	// the announcement is out, the flag is down — and the pairing with the
	// test below, where an announcement that never went out keeps it up.
	asked := make(chan bool, 1)
	go func() {
		<-s.events
		asked <- s.PendingOverflow()
	}()
	// Retried until it lands: the announcement is sent without waiting, so it
	// finds no reader until that goroutine is parked on the channel.
	for i := 0; i < 1000 && s.PendingOverflow(); i++ {
		s.flushDropped()
		time.Sleep(time.Millisecond)
	}

	select {
	case still := <-asked:
		if still {
			t.Error("the flag still stood as the announcement was taken; that refusal is counted twice, and the flag left standing swallows the next announcement whole")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the announcement never went out")
	}
}

// A refusal that cannot be announced keeps the flag, which is the only thing
// the flag is for: the bridge asks, and goes and reads the window.
func TestAnOverflowThatCannotBeAnnouncedKeepsItsFlag(t *testing.T) {
	s := &socketModeStream{events: make(chan StreamEvent)}

	s.dropped.Store(true)
	s.flushDropped()

	if !s.PendingOverflow() {
		t.Error("the flag was cleared by an announcement that never went out")
	}
}
