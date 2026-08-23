package bridge

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

// screenshot is the file the owner attached, in the shape Slack sends it:
// trimmed of the thumbnails and preview fields the bridge does not report, but
// otherwise a real payload.
const screenshot = `{
  "id": "F0FILEID",
  "created": 1787504915,
  "name": "image.png",
  "title": "image.png",
  "mimetype": "image/png",
  "filetype": "png",
  "user": "U0OWNER",
  "size": 88731,
  "mode": "hosted",
  "url_private": "https://files.slack.com/files-pri/T0TEAM-F0FILEID/image.png",
  "url_private_download": "https://files.slack.com/files-pri/T0TEAM-F0FILEID/download/image.png",
  "permalink": "https://example.slack.com/files/U0OWNER/F0FILEID/image.png",
  "permalink_public": "https://slack-files.com/T0TEAM-F0FILEID-30c935ff92"
}`

// wantScreenshot is what the file above must arrive as.
var wantScreenshot = File{
	Name:       "image.png",
	Mimetype:   "image/png",
	Size:       88731,
	URLPrivate: "https://files.slack.com/files-pri/T0TEAM-F0FILEID/image.png",
	Permalink:  "https://example.slack.com/files/U0OWNER/F0FILEID/image.png",
}

// eventsAPIEnvelope builds the Socket Mode event the bridge would receive for
// one raw events_api payload, parsed exactly the way the socketmode client
// parses it. The raw payload is kept alongside the typed event because that is
// where the file list lives: slackevents.MessageEvent does not model one.
func eventsAPIEnvelope(t *testing.T, payload string) socketmode.Event {
	t.Helper()

	raw := json.RawMessage(payload)
	api, err := slackevents.ParseEvent(raw, slackevents.OptionNoVerifyToken())
	if err != nil {
		t.Fatalf("parsing the events_api payload: %v", err)
	}
	return socketmode.Event{
		Type:    socketmode.EventTypeEventsAPI,
		Data:    api,
		Request: &socketmode.Request{Payload: raw},
	}
}

func fileShareEnvelope(t *testing.T, user, text string) socketmode.Event {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"type": "event_callback",
		"event": map[string]any{
			"type":      "message",
			"subtype":   "file_share",
			"channel":   testChannel,
			"user":      user,
			"text":      text,
			"ts":        "100.000200",
			"thread_ts": "100.000100",
			"files":     []json.RawMessage{json.RawMessage(screenshot)},
		},
	})
	if err != nil {
		t.Fatalf("building the payload: %v", err)
	}
	return eventsAPIEnvelope(t, string(payload))
}

// The gap this fixes: the owner sends a screenshot from their phone and the
// bridge says nothing at all, because file_share was filtered out as one of
// Slack's housekeeping notices.
func TestLiveFileShareIsDeliveredWithItsFiles(t *testing.T) {
	stream := newTestStream(4)

	stream.handle(func(socketmode.Request) {}, fileShareEnvelope(t, testOwner, "画像ってみえますか"))

	select {
	case evt := <-stream.events:
		if evt.Kind != StreamMessage {
			t.Fatalf("event kind = %v, want StreamMessage", evt.Kind)
		}
		if evt.Message.Text != "画像ってみえますか" {
			t.Errorf("Text = %q, want the caption relayed unchanged", evt.Message.Text)
		}
		if evt.Message.ThreadTS != "100.000100" {
			t.Errorf("ThreadTS = %q, want the thread it was sent in", evt.Message.ThreadTS)
		}
		if !reflect.DeepEqual(evt.Message.Files, []File{wantScreenshot}) {
			t.Errorf("Files = %+v, want %+v", evt.Message.Files, []File{wantScreenshot})
		}
	default:
		t.Fatal("nothing was delivered for a file the owner shared")
	}
}

// An upload sent with no caption has no text at all, and dropping it for
// emptiness would be the same silence in a different place.
func TestLiveFileShareWithNoCaptionIsStillDelivered(t *testing.T) {
	stream := newTestStream(4)

	stream.handle(func(socketmode.Request) {}, fileShareEnvelope(t, testOwner, ""))

	select {
	case evt := <-stream.events:
		if evt.Message.Text != "" {
			t.Errorf("Text = %q, want it empty", evt.Message.Text)
		}
		if len(evt.Message.Files) != 1 {
			t.Fatalf("Files = %+v, want the one attachment", evt.Message.Files)
		}
	default:
		t.Fatal("an upload with no caption was dropped; the files are the message")
	}
}

// Accepting file_share must not widen who reaches the agent.
func TestLiveFileShareFromSomebodyElseIsNotDelivered(t *testing.T) {
	stream := newTestStream(4)

	stream.handle(func(socketmode.Request) {}, fileShareEnvelope(t, "U0SOMEONE", "look at this"))

	select {
	case evt := <-stream.events:
		t.Fatalf("delivered %+v, want nothing: somebody else shared that file", evt.Message)
	default:
	}
}

// A plain message has no files array, and must stay exactly as it was.
func TestLivePlainMessageCarriesNoFiles(t *testing.T) {
	stream := newTestStream(4)

	stream.handle(func(socketmode.Request) {}, eventsAPIEnvelope(t, `{
	  "type": "event_callback",
	  "event": {
	    "type": "message",
	    "channel": "`+testChannel+`",
	    "user": "`+testOwner+`",
	    "text": "just talking",
	    "ts": "100.000300"
	  }
	}`))

	select {
	case evt := <-stream.events:
		if evt.Message.Files != nil {
			t.Errorf("Files = %+v, want nil so a plain message is unchanged on the wire", evt.Message.Files)
		}
	default:
		t.Fatal("a plain owner message was not delivered")
	}
}

// A mention carrying an attachment comes in as app_mention, which unlike the
// message event does model its files.
func TestLiveAppMentionCarriesItsFiles(t *testing.T) {
	stream := newTestStream(4)

	stream.handle(func(socketmode.Request) {}, eventsAPIEnvelope(t, `{
	  "type": "event_callback",
	  "event": {
	    "type": "app_mention",
	    "channel": "C0ELSEWHERE",
	    "user": "`+testOwner+`",
	    "text": "<@U0BOT> what is this",
	    "ts": "100.000400",
	    "files": [`+screenshot+`]
	  }
	}`))

	select {
	case evt := <-stream.events:
		if !reflect.DeepEqual(evt.Message.Files, []File{wantScreenshot}) {
			t.Errorf("Files = %+v, want %+v", evt.Message.Files, []File{wantScreenshot})
		}
	default:
		t.Fatal("a mention with an attachment was dropped")
	}
}

// Catch-up reads the same upload back from conversations.history, where Slack
// reports it without the file_share subtype. Both shapes have to arrive, or a
// message delivered by one path is lost by the other.
func TestCatchUpDeliversAnUploadWithItsFiles(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	shared := ownerMsg("100.000200", "look at this")
	shared.Files = []File{wantScreenshot}
	// The same upload as Slack files it away: no subtype, only the files.
	captionless := candidate{Channel: testChannel, User: testOwner, TS: "100.000300", Files: []File{wantScreenshot}}

	api := &fakeAPI{history: []candidate{
		ownerMsg("100.000100", "already answered"),
		shared,
		captionless,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("Wait() returned %d messages, want both uploads", len(result.Messages))
	}
	for _, m := range result.Messages {
		if !reflect.DeepEqual(m.Files, []File{wantScreenshot}) {
			t.Errorf("message %s Files = %+v, want %+v", m.TS, m.Files, []File{wantScreenshot})
		}
	}
}

// slack_history reports attachments too: the owner asking what somebody posted
// should not be told about a message whose whole content was a file.
func TestHistoryReportsAttachments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api := historyBridge(ctx, t)
	api.mu.Lock()
	shared := candidate{Channel: testChannel, User: "U0COLLEAGUE", Text: "the graph", TS: "100.000600", Files: []File{wantScreenshot}}
	api.history = append(api.history, shared)
	api.mu.Unlock()

	result, err := b.History(ctx, ReadRequest{})
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}

	var found bool
	for _, m := range result.Messages {
		if m.TS != "100.000600" {
			if m.Files != nil {
				t.Errorf("message %s carries Files = %+v, want nil", m.TS, m.Files)
			}
			continue
		}
		found = true
		if !reflect.DeepEqual(m.Files, []File{wantScreenshot}) {
			t.Errorf("Files = %+v, want %+v", m.Files, []File{wantScreenshot})
		}
	}
	if !found {
		t.Error("the message with the attachment was not returned at all")
	}
}

// The Web API path narrows Slack's file object down to the reported fields.
func TestToCandidateCarriesFileMetadata(t *testing.T) {
	var file slack.File
	if err := json.Unmarshal([]byte(screenshot), &file); err != nil {
		t.Fatalf("unmarshalling the file: %v", err)
	}

	got := toCandidate(testChannel, slack.Message{Msg: slack.Msg{
		User:      testOwner,
		Text:      "look at this",
		Timestamp: "100.000200",
		Files:     []slack.File{file},
	}})

	if !reflect.DeepEqual(got.Files, []File{wantScreenshot}) {
		t.Errorf("toCandidate() Files = %+v, want %+v", got.Files, []File{wantScreenshot})
	}
	if plain := toCandidate(testChannel, slack.Message{Msg: slack.Msg{Text: "hi"}}); plain.Files != nil {
		t.Errorf("toCandidate() Files = %+v for a message with no attachment, want nil", plain.Files)
	}
}

// The raw envelope is the only place a message event's files exist, and the
// reader has to survive every other envelope shape Slack sends it.
func TestFilesFromEnvelopeIgnoresWhatItCannotRead(t *testing.T) {
	for _, payload := range []string{"", "null", `"a string payload"`, `[1,2,3]`, `{"event":{}}`, `{"event":{"files":[]}}`, "{not json"} {
		if got := filesFromEnvelope(json.RawMessage(payload)); got != nil {
			t.Errorf("filesFromEnvelope(%q) = %+v, want nil", payload, got)
		}
	}
}

// The delivered message keeps its files all the way through the wait, rather
// than being rebuilt from text somewhere along the way.
func TestLiveFileShareSurvivesTheWait(t *testing.T) {
	cfg := testConfig(t)
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the cursor: %v", err)
	}

	stream := newFakeStream()
	api := &fakeAPI{history: []candidate{ownerMsg("100.000100", "already answered")}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	defer func() { _ = b.Close() }()

	go func() {
		time.Sleep(20 * time.Millisecond)
		stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{
			TS:      "100.000200",
			User:    testOwner,
			Channel: testChannel,
			Files:   []File{wantScreenshot},
		}}
	}()

	result, err := b.Wait(ctx, MaxWaitTimeout)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(result.Messages) != 1 {
		t.Fatalf("Wait() returned %d messages, want the upload", len(result.Messages))
	}
	if !reflect.DeepEqual(result.Messages[0].Files, []File{wantScreenshot}) {
		t.Errorf("Files = %+v, want %+v", result.Messages[0].Files, []File{wantScreenshot})
	}
}
