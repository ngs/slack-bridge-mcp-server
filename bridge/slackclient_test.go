package bridge

import "testing"

func newTestStream(size int) *socketModeStream {
	return &socketModeStream{
		events:       make(chan StreamEvent, size),
		interactions: make(chan Interaction, size),
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
