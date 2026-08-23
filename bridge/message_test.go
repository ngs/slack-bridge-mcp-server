package bridge

import (
	"reflect"
	"testing"
)

const (
	testChannel = "C0BRIDGE"
	testOwner   = "U0OWNER"
)

// accept is the security boundary: it is what keeps other people in the
// channel, Slack's own housekeeping notices, and the agent's own posts from
// being fed back to the model as owner instructions.
func TestAcceptFiltersToOwnerMessages(t *testing.T) {
	tests := []struct {
		name string
		in   candidate
		want bool
	}{
		{
			name: "plain owner message",
			in:   candidate{Channel: testChannel, User: testOwner, Text: "hello", TS: "100.000100"},
			want: true,
		},
		{
			name: "thread reply carries thread_ts",
			in:   candidate{Channel: testChannel, User: testOwner, Text: "in thread", TS: "100.000200", ThreadTS: "100.000100"},
			want: true,
		},
		{
			name: "thread_broadcast is still the owner speaking",
			in:   candidate{Channel: testChannel, User: testOwner, Text: "also here", TS: "100.000300", ThreadTS: "100.000100", SubType: "thread_broadcast"},
			want: true,
		},
		{
			name: "another member of the channel",
			in:   candidate{Channel: testChannel, User: "U0SOMEONE", Text: "hi", TS: "100.000400"},
			want: false,
		},
		{
			name: "our own echo, posted by the bot under the owner's app",
			in:   candidate{Channel: testChannel, User: testOwner, BotID: "B0BRIDGE", Text: "reply", TS: "100.000500"},
			want: false,
		},
		{
			name: "a bot with no user at all",
			in:   candidate{Channel: testChannel, BotID: "B0OTHER", Text: "deploy finished", TS: "100.000600"},
			want: false,
		},
		{
			name: "an edit of an earlier message",
			in:   candidate{Channel: testChannel, User: testOwner, Text: "edited", TS: "100.000700", SubType: "message_changed"},
			want: false,
		},
		{
			name: "a deletion",
			in:   candidate{Channel: testChannel, User: testOwner, TS: "100.000800", SubType: "message_deleted"},
			want: false,
		},
		{
			name: "a join notice",
			in:   candidate{Channel: testChannel, User: testOwner, Text: "has joined the channel", TS: "100.000900", SubType: "channel_join"},
			want: false,
		},
		{
			name: "a file share is the owner handing something over",
			in: candidate{
				Channel: testChannel, User: testOwner, Text: "look at this", TS: "100.001000", SubType: "file_share",
				Files: []File{{Name: "screenshot.png", Mimetype: "image/png"}},
			},
			want: true,
		},
		{
			name: "an upload with no caption still says something",
			in: candidate{
				Channel: testChannel, User: testOwner, TS: "100.001050", SubType: "file_share",
				Files: []File{{Name: "screenshot.png", Mimetype: "image/png"}},
			},
			want: true,
		},
		{
			name: "somebody else's file share is nobody's business",
			in: candidate{
				Channel: testChannel, User: "U0SOMEONE", Text: "look at this", TS: "100.001060", SubType: "file_share",
				Files: []File{{Name: "screenshot.png", Mimetype: "image/png"}},
			},
			want: false,
		},
		{
			name: "an app posting a file is still an app",
			in: candidate{
				Channel: testChannel, User: testOwner, BotID: "B0BRIDGE", Text: "here is the report", TS: "100.001070", SubType: "file_share",
				Files: []File{{Name: "report.pdf", Mimetype: "application/pdf"}},
			},
			want: false,
		},
		{
			name: "the owner talking in a different channel",
			in:   candidate{Channel: "C0OTHER", User: testOwner, Text: "not for the bridge", TS: "100.001100"},
			want: false,
		},
		{
			name: "an empty body has nothing to relay",
			in:   candidate{Channel: testChannel, User: testOwner, Text: "   ", TS: "100.001200"},
			want: false,
		},
		{
			name: "no timestamp means no cursor and no ack target",
			in:   candidate{Channel: testChannel, User: testOwner, Text: "hello"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := accept(tt.in, testChannel, testOwner)
			if ok != tt.want {
				t.Fatalf("accept() ok = %v, want %v", ok, tt.want)
			}
			if !ok {
				return
			}
			if got.TS != tt.in.TS || got.User != tt.in.User || got.Text != tt.in.Text {
				t.Errorf("accept() = %+v, want it to carry ts/user/text through unchanged", got)
			}
			if got.ThreadTS != tt.in.ThreadTS {
				t.Errorf("accept() ThreadTS = %q, want %q so the caller can reply in the thread", got.ThreadTS, tt.in.ThreadTS)
			}
			if !reflect.DeepEqual(got.Files, tt.in.Files) {
				t.Errorf("accept() Files = %+v, want %+v", got.Files, tt.in.Files)
			}
		})
	}
}

// An unconfigured bridge must not relay anything. Without this, an empty owner
// would match the empty User on Slack's own bot notices.
//
// The channel is a different matter, and deliberately so: an empty one means
// any channel, which is what the live stream passes. Which conversations are
// open changes while the session runs, so that decision belongs to the bridge
// rather than to the socket.
func TestAcceptRejectsEverythingWhenUnconfigured(t *testing.T) {
	c := candidate{Channel: testChannel, User: testOwner, Text: "hello", TS: "100.000100"}

	if _, ok := accept(c, "", testOwner); !ok {
		t.Error("accept() rejected a message when no channel was asked for; want any channel accepted")
	}
	if _, ok := accept(c, testChannel, ""); ok {
		t.Error("accept() with no owner configured returned a message")
	}
	if _, ok := accept(candidate{Channel: testChannel, Text: "x", TS: "1.0"}, testChannel, ""); ok {
		t.Error("accept() matched a message with no author against an empty owner")
	}
}

// Slack timestamps are compared numerically rather than lexically, so ordering
// keeps working when the seconds field gains a digit.
func TestTSLess(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"1723456789.000100", "1723456789.000200", true},
		{"1723456789.000200", "1723456789.000100", false},
		{"1723456789.000100", "1723456789.000100", false},
		{"1723456789.000900", "1723456790.000000", true},
		// The digit rollover a string comparison gets wrong: "9999999999" sorts
		// after "10000000000" lexically, but is earlier in time.
		{"9999999999.000000", "10000000000.000000", true},
		{"10000000000.000000", "9999999999.000000", false},
		// Sequence numbers compare as integers, not as text.
		{"100.000099", "100.000100", true},
		{"100.0000100", "100.0000099", false},
		// Malformed input still yields a total order rather than a panic.
		{"", "100.000100", true},
		{"garbage", "100.000100", false},
	}

	for _, tt := range tests {
		if got := tsLess(tt.a, tt.b); got != tt.want {
			t.Errorf("tsLess(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

// Catch-up and the live stream overlap around a reconnect, so the merge has to
// produce one oldest-first run with no repeats.
func TestMergeMessagesOrdersDeduplicatesAndTrims(t *testing.T) {
	// As conversations.history returns them: newest first.
	history := []Message{
		{TS: "100.000300", Text: "third"},
		{TS: "100.000200", Text: "second"},
		{TS: "100.000100", Text: "first"},
	}
	// The same reconnect also delivered the last two over the WebSocket, plus
	// one that history had not caught yet.
	live := []Message{
		{TS: "100.000200", Text: "second"},
		{TS: "100.000300", Text: "third"},
		{TS: "100.000400", Text: "fourth"},
	}

	got := mergeMessages("", history, live)

	want := []Message{
		{TS: "100.000100", Text: "first"},
		{TS: "100.000200", Text: "second"},
		{TS: "100.000300", Text: "third"},
		{TS: "100.000400", Text: "fourth"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mergeMessages() = %+v\nwant %+v", got, want)
	}
}

func TestMergeMessagesDropsAnythingAtOrBeforeTheCursor(t *testing.T) {
	msgs := []Message{
		{TS: "100.000100", Text: "already answered"},
		{TS: "100.000200", Text: "the cursor itself"},
		{TS: "100.000300", Text: "new"},
	}

	got := mergeMessages("100.000200", msgs)

	if len(got) != 1 || got[0].TS != "100.000300" {
		t.Errorf("mergeMessages() = %+v, want only the message after the cursor", got)
	}
}

func TestMergeMessagesHandlesEmptyInput(t *testing.T) {
	if got := mergeMessages(""); len(got) != 0 {
		t.Errorf("mergeMessages() with no sources = %+v, want empty", got)
	}
	if got := mergeMessages("100.000100", nil, []Message{}); len(got) != 0 {
		t.Errorf("mergeMessages() with empty sources = %+v, want empty", got)
	}
	// A message with no timestamp cannot be ordered or acknowledged.
	if got := mergeMessages("", []Message{{Text: "no ts"}}); len(got) != 0 {
		t.Errorf("mergeMessages() = %+v, want messages without a ts dropped", got)
	}
}
