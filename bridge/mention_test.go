package bridge

import (
	"context"
	"strconv"
	"testing"
	"time"
)

const (
	// otherChannel is a channel the bot has been added to that is not the
	// owner's home channel: somebody else's project channel, where the agent is
	// a guest and only speaks when spoken to.
	otherChannel = "C0PROJECT"
	testBotUser  = "U0BOT"
	colleague    = "U0COLLEAGUE"
)

// mention writes what Slack puts in the text when the owner types the bot's
// name.
func mention(text string) string { return "<@" + testBotUser + "> " + text }

// mentionBridge is a bridge that has already read the home channel, so that
// what the tests see next is only what they put there themselves. The
// indicator and the receipt reaction are turned off: both are covered
// elsewhere, and both would otherwise post into the assertions.
func mentionBridge(ctx context.Context, t *testing.T) (*Bridge, *fakeAPI, *fakeStream) {
	t.Helper()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the home cursor: %v", err)
	}

	api := &fakeAPI{
		botUserID: testBotUser,
		postTS:    "100.000900",
		channelHistory: map[string][]candidate{
			testChannel: {ownerMsg("100.000100", "already answered")},
		},
	}

	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })
	return b, api, stream
}

// send puts a message on the live stream the way the socket would, having
// already applied the rules that hold everywhere.
func send(stream *fakeStream, channel, ts, threadTS, text string) {
	stream.events <- StreamEvent{Kind: StreamMessage, Message: Message{
		TS: ts, ThreadTS: threadTS, Channel: channel, User: testOwner, Text: text,
	}}
}

// waitFor collects one batch, or none if the bridge has nothing to hand over.
func waitFor(ctx context.Context, t *testing.T, b *Bridge) []Message {
	t.Helper()

	result, err := b.Wait(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return result.Messages
}

// The whole of the new behaviour in one conversation: the owner mentions the
// bot in a channel that is not theirs, which opens a thread; from then on they
// talk in that thread without mentioning it again; and the channel around them
// carries on without the agent hearing any of it.
func TestAMentionOpensAConversationThread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	send(stream, otherChannel, "200.000100", "", mention("can you look at the deploy?"))
	msgs := waitFor(ctx, t, b)
	if len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the mention", texts(msgs))
	}
	if msgs[0].Channel != otherChannel {
		t.Errorf("message channel = %q, want %q so the reply can find its way back", msgs[0].Channel, otherChannel)
	}
	if msgs[0].ThreadTS != "200.000100" {
		t.Errorf("message thread_ts = %q, want the mention's own ts: the conversation goes in the thread under it", msgs[0].ThreadTS)
	}

	// Inside the thread, no further mention is needed.
	send(stream, otherChannel, "200.000200", "200.000100", "and the rollback too")
	msgs = waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "200.000200" {
		t.Fatalf("Wait() returned %v, want the thread reply relayed without another mention", texts(msgs))
	}
	if msgs[0].Channel != otherChannel || msgs[0].ThreadTS != "200.000100" {
		t.Errorf("thread reply = %+v, want it to carry its channel and thread", msgs[0])
	}

	// Out on the channel surface, the owner is talking to their colleagues.
	send(stream, otherChannel, "200.000300", "", "morning everyone")
	if msgs := waitFor(ctx, t, b); len(msgs) != 0 {
		t.Errorf("Wait() returned %v, want nothing: a message outside the conversation is not addressed to the agent", texts(msgs))
	}

	// A different thread in the same channel is a different conversation.
	send(stream, otherChannel, "200.000500", "200.000400", "what do you think?")
	if msgs := waitFor(ctx, t, b); len(msgs) != 0 {
		t.Errorf("Wait() returned %v, want nothing from a thread the agent was never invited into", texts(msgs))
	}
}

// The home channel is the owner's own, and it keeps working the way it always
// has: no mention, no thread, everything relayed.
func TestTheHomeChannelStillNeedsNoMention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)

	send(stream, testChannel, "100.000200", "", "no mention needed here")
	msgs := waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "100.000200" {
		t.Fatalf("Wait() returned %v, want the home channel message relayed as always", texts(msgs))
	}
	if msgs[0].Channel != testChannel {
		t.Errorf("message channel = %q, want the home channel named like any other", msgs[0].Channel)
	}
	if msgs[0].ThreadTS != "" {
		t.Errorf("message thread_ts = %q, want it empty: a surface message in the home channel is answered on the surface", msgs[0].ThreadTS)
	}
}

// A mention is what the owner types, not what anyone types. The socket applies
// the owner filter before the bridge ever sees a message, which is what keeps
// a colleague from opening a conversation — or joining one.
func TestOnlyTheOwnerIsRelayed(t *testing.T) {
	for _, tt := range []struct {
		name string
		c    candidate
	}{
		{"a colleague mentioning the bot", candidate{
			Channel: otherChannel, User: colleague, Text: mention("do this for me"), TS: "200.000100",
		}},
		{"a colleague replying inside an open thread", candidate{
			Channel: otherChannel, User: colleague, Text: "here is a thought", TS: "200.000200", ThreadTS: "200.000100",
		}},
		{"an app posting the mention", candidate{
			Channel: otherChannel, User: testOwner, BotID: "B0APP", Text: mention("automated"), TS: "200.000300",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := accept(tt.c, "", testOwner); ok {
				t.Errorf("accept(%+v) relayed a message the owner did not write", tt.c)
			}
		})
	}
}

// A conversation is a thread, so what is said in it has to come back from the
// thread — and only the owner's half of it.
func TestAThreadCatchUpRelaysOnlyTheOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := mentionBridge(ctx, t)

	send(stream, otherChannel, "200.000100", "", mention("take a look"))
	if msgs := waitFor(ctx, t, b); len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the mention", texts(msgs))
	}

	// While the socket was down, both of them said something in the thread.
	api.mu.Lock()
	api.replies = []candidate{
		{Channel: otherChannel, User: colleague, Text: "I have a theory", TS: "200.000200", ThreadTS: "200.000100"},
		{Channel: otherChannel, User: testOwner, Text: "any progress?", TS: "200.000300", ThreadTS: "200.000100"},
	}
	api.mu.Unlock()
	stream.events <- StreamEvent{Kind: StreamConnected}

	msgs := waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "200.000300" {
		t.Fatalf("Wait() returned %v, want only the owner's reply recovered", texts(msgs))
	}

	// And nothing comes back twice on the next reconnect.
	stream.events <- StreamEvent{Kind: StreamConnected}
	if msgs := waitFor(ctx, t, b); len(msgs) != 0 {
		t.Errorf("Wait() returned %v on a second catch-up, want the thread cursor to have moved past it", texts(msgs))
	}
}

// A conversation outlives the session it started in: the owner should not have
// to mention the bot again because the laptop slept.
func TestAnOpenConversationSurvivesARestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	stateDir := b.cfg.StateDir

	send(stream, otherChannel, "200.000100", "", mention("look into this please"))
	if msgs := waitFor(ctx, t, b); len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the mention", texts(msgs))
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// A new process, the same state directory, and a reply left in the thread
	// in the meantime.
	cfg := testConfig(t)
	cfg.StateDir = stateDir
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	api := &fakeAPI{
		botUserID:      testBotUser,
		channelHistory: map[string][]candidate{testChannel: {ownerMsg("100.000100", "already answered")}},
		replies: []candidate{
			{Channel: otherChannel, User: testOwner, Text: "still there?", TS: "200.000400", ThreadTS: "200.000100"},
		},
	}
	restarted := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = restarted.Close() })

	msgs := waitFor(ctx, t, restarted)
	if len(msgs) != 1 || msgs[0].TS != "200.000400" {
		t.Fatalf("Wait() after a restart returned %v, want the reply left in the open conversation", texts(msgs))
	}
	if msgs[0].Channel != otherChannel || msgs[0].ThreadTS != "200.000100" {
		t.Errorf("recovered reply = %+v, want it to carry its channel and thread", msgs[0])
	}
}

// The mention that opened the conversation was delivered by the session that
// is gone. Handing it over again would have the agent answer a question it has
// already answered.
func TestARestartDoesNotReplayTheOpeningMention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _, stream := mentionBridge(ctx, t)
	stateDir := b.cfg.StateDir

	send(stream, otherChannel, "200.000100", "", mention("look into this please"))
	if msgs := waitFor(ctx, t, b); len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the mention", texts(msgs))
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	cfg := testConfig(t)
	cfg.StateDir = stateDir
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	// Slack still has the whole thread, opening message included, and the scan
	// still has the whole channel to look at.
	api := &fakeAPI{
		botUserID: testBotUser,
		joined:    []string{testChannel, otherChannel},
		channelHistory: map[string][]candidate{
			testChannel:  {ownerMsg("100.000100", "already answered")},
			otherChannel: {{Channel: otherChannel, User: testOwner, Text: mention("look into this please"), TS: "200.000100"}},
		},
		replies: []candidate{
			{Channel: otherChannel, User: testOwner, Text: mention("look into this please"), TS: "200.000100"},
		},
	}
	restarted := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = restarted.Close() })

	if msgs := waitFor(ctx, t, restarted); len(msgs) != 0 {
		t.Errorf("Wait() after a restart returned %v, want nothing: all of that was already delivered", texts(msgs))
	}
}

// The sleep-tolerance story outside the home channel: a mention that arrived
// while nothing was listening is found by looking at the channels the bot is
// in, and the conversation it opens works from then on like any other.
func TestAMentionSentWhileAsleepIsFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the home cursor: %v", err)
	}
	// The bridge has looked for mentions before, which is what makes this a
	// search rather than a first run.
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatalf("seeding the mention cursor: %v", err)
	}

	api := &fakeAPI{
		botUserID: testBotUser,
		joined:    []string{otherChannel, testChannel},
		channelHistory: map[string][]candidate{
			testChannel: {ownerMsg("100.000100", "already answered")},
			otherChannel: {
				{Channel: otherChannel, User: colleague, Text: "unrelated chatter", TS: "300.000100"},
				{Channel: otherChannel, User: testOwner, Text: "not addressed to the bot", TS: "300.000200"},
				{Channel: otherChannel, User: testOwner, Text: mention("while you were away"), TS: "300.000300"},
			},
		},
	}
	stream := newFakeStream()
	b := New(ctx, cfg, &fakeConnector{api: api, stream: stream})
	t.Cleanup(func() { _ = b.Close() })

	msgs := waitFor(ctx, t, b)
	if len(msgs) != 1 || msgs[0].TS != "300.000300" {
		t.Fatalf("Wait() returned %v, want the mention sent while the session was down", texts(msgs))
	}
	if msgs[0].Channel != otherChannel || msgs[0].ThreadTS != "300.000300" {
		t.Errorf("recovered mention = %+v, want its channel and its own ts as the thread", msgs[0])
	}

	// The conversation is open, so the owner's next line needs no mention.
	send(stream, otherChannel, "300.000400", "300.000300", "so what do you make of it?")
	if msgs := waitFor(ctx, t, b); len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the thread reply after the recovered mention", texts(msgs))
	}
}

// A first run joins the workspace rather than replaying it. Without this, every
// mention the bot was ever sent would arrive at once the first time it starts.
func TestTheFirstRunSeedsTheMentionCursorInsteadOfReplaying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true
	if err := NewStore(cfg.StateDir).SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the home cursor: %v", err)
	}

	api := &fakeAPI{
		botUserID: testBotUser,
		joined:    []string{otherChannel},
		channelHistory: map[string][]candidate{
			testChannel:  {ownerMsg("100.000100", "already answered")},
			otherChannel: {{Channel: otherChannel, User: testOwner, Text: mention("from last month"), TS: "300.000100"}},
		},
	}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	if msgs := waitFor(ctx, t, b); len(msgs) != 0 {
		t.Errorf("Wait() on a first run returned %v, want the workspace's history left where it is", texts(msgs))
	}

	cursor, err := NewStore(cfg.StateDir).MentionCursor()
	if err != nil {
		t.Fatalf("MentionCursor() error = %v", err)
	}
	if cursor != "300.000100" {
		t.Errorf("mention cursor = %q, want the newest message the scan saw", cursor)
	}
}

// The search is bounded, because a workspace can hold hundreds of channels and
// this runs on every reconnect. What is dropped is dropped out loud.
func TestTheMentionSearchIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testConfig(t)
	cfg.IndicatorDisabled = true
	cfg.AutoAckDisabled = true

	store := NewStore(cfg.StateDir)
	if err := store.SetLastTS(testChannel, "100.000100"); err != nil {
		t.Fatalf("seeding the home cursor: %v", err)
	}
	if err := store.SetMentionCursor("100.000100"); err != nil {
		t.Fatalf("seeding the mention cursor: %v", err)
	}

	history := map[string][]candidate{testChannel: {ownerMsg("100.000100", "already answered")}}
	joined := make([]string, 0, maxScannedChannels+5)
	for i := 0; i < maxScannedChannels+5; i++ {
		id := "C0BUSY" + strconv.Itoa(i)
		joined = append(joined, id)
		history[id] = nil
	}

	api := &fakeAPI{botUserID: testBotUser, joined: joined, channelHistory: history}
	b := New(ctx, cfg, &fakeConnector{api: api, stream: newFakeStream()})
	t.Cleanup(func() { _ = b.Close() })

	waitFor(ctx, t, b)

	api.mu.Lock()
	defer api.mu.Unlock()

	scanned := make(map[string]bool)
	for _, call := range api.historyCalls {
		if call.Limit == mentionScanPageLimit {
			scanned[call.Channel] = true
		}
	}
	if len(scanned) != maxScannedChannels {
		t.Errorf("the search read %d channels, want it capped at %d", len(scanned), maxScannedChannels)
	}
	if got := api.joinedCalls; len(got) != 1 || got[0] != maxScannedChannels {
		t.Errorf("users.conversations calls = %v, want one asking for %d channels", got, maxScannedChannels)
	}
}

// Without its own user ID the bridge cannot tell a mention from any other
// message. The home channel still works, and nothing else opens — which is the
// safe way round: a bridge that guessed would relay a colleague's channel.
func TestNoConversationOpensWithoutTheBotID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := mentionBridge(ctx, t)
	api.mu.Lock()
	api.botUserID = ""
	api.mu.Unlock()

	// Reconnecting is what makes the bridge read the ID again.
	if err := b.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	send(stream, otherChannel, "200.000100", "", mention("anyone home?"))
	if msgs := waitFor(ctx, t, b); len(msgs) != 0 {
		t.Errorf("Wait() returned %v, want nothing: an unrecognisable mention opens nothing", texts(msgs))
	}
}

// Answering in the wrong place is worse than not answering: the owner is
// watching a thread in one channel and the reply goes to another. Every tool
// that speaks takes the channel back, and every one of them still means the
// home channel when it is left out.
func TestRepliesGoToTheChannelTheyAreAddressedTo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := mentionBridge(ctx, t)

	if _, err := b.Post(ctx, PostRequest{Text: "on it", ThreadTS: "200.000100", Channel: otherChannel}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}
	if _, err := b.Post(ctx, PostRequest{Text: "and here at home"}); err != nil {
		t.Fatalf("Post() to the default channel error = %v", err)
	}

	posts := api.snapshotPosts()
	if len(posts) != 2 {
		t.Fatalf("posts = %+v, want two", posts)
	}
	if posts[0].Channel != otherChannel || posts[0].ThreadTS != "200.000100" {
		t.Errorf("first post = %+v, want it in the thread it answers", posts[0])
	}
	if posts[1].Channel != testChannel {
		t.Errorf("second post = %+v, want the home channel when none is named", posts[1])
	}

	if err := b.React(ctx, ReactRequest{TS: "200.000100", Emoji: "eyes", Channel: otherChannel}); err != nil {
		t.Fatalf("React() error = %v", err)
	}
	if err := b.React(ctx, ReactRequest{TS: "100.000200", Emoji: "eyes"}); err != nil {
		t.Fatalf("React() to the default channel error = %v", err)
	}

	api.mu.Lock()
	reactions := append([]reactionCall(nil), api.reactions...)
	api.mu.Unlock()

	if len(reactions) != 2 {
		t.Fatalf("reactions = %+v, want two", reactions)
	}
	if reactions[0].Channel != otherChannel {
		t.Errorf("first reaction = %+v, want it on the message it marks", reactions[0])
	}
	if reactions[1].Channel != testChannel {
		t.Errorf("second reaction = %+v, want the home channel when none is named", reactions[1])
	}
}

// A question asked in a conversation has to be asked there, and answered from
// there: a click is matched against the message it belongs to, and that message
// is no longer always in the home channel.
func TestAQuestionIsAskedAndAnsweredInItsOwnChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := mentionBridge(ctx, t)
	api.mu.Lock()
	api.questionTS = "200.000700"
	api.mu.Unlock()

	answered := make(chan AskResult, 1)
	go func() {
		result, err := b.Ask(ctx, AskRequest{
			Question: "Ship it?",
			Options:  []string{"Yes", "No"},
			Timeout:  MaxWaitTimeout,
			ThreadTS: "200.000100",
			Channel:  otherChannel,
		})
		if err != nil {
			t.Errorf("Ask() error = %v", err)
		}
		answered <- result
	}()

	eventually(t, "the question to be posted", func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.questions) == 1
	})

	api.mu.Lock()
	asked := api.questions[0]
	api.mu.Unlock()
	if asked.Channel != otherChannel || asked.ThreadTS != "200.000100" {
		t.Errorf("question posted as %+v, want it in the conversation it belongs to", asked)
	}

	stream.interactions <- Interaction{
		User:      testOwner,
		Channel:   otherChannel,
		MessageTS: "200.000700",
		BlockID:   askBlockID,
		ActionID:  askActionPrefix + "0",
		Value:     "0",
	}

	select {
	case result := <-answered:
		if result.ChoiceIndex != 0 {
			t.Errorf("Ask() = %+v, want the click from the question's own channel to answer it", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the click never reached the question")
	}
}

// The two things the server does on the agent's behalf — the receipt reaction
// and the elapsed-time indicator — have to happen where the owner is looking,
// which is no longer always the home channel.
func TestTheReceiptAndTheIndicatorFollowTheConversation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, stream := mentionBridge(ctx, t)
	b.cfg.AutoAckDisabled = false
	b.cfg.IndicatorDisabled = false
	b.cfg.IndicatorGrace = testGrace
	b.cfg.IndicatorInterval = testInterval

	send(stream, otherChannel, "200.000100", "", mention("have a look at this"))
	if msgs := waitFor(ctx, t, b); len(msgs) != 1 {
		t.Fatalf("Wait() returned %v, want the mention", texts(msgs))
	}

	eventually(t, "the receipt to be added", func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		return len(api.reactions) == 1
	})
	api.mu.Lock()
	receipt := api.reactions[0]
	api.mu.Unlock()
	if receipt.Channel != otherChannel || receipt.TS != "200.000100" {
		t.Errorf("receipt = %+v, want it on the message that asked, in its own channel", receipt)
	}

	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })
	if got := indicatorPosts(api)[0]; got.Channel != otherChannel || got.ThreadTS != "200.000100" {
		t.Errorf("indicator posted as %+v, want it in the conversation the owner is watching", got)
	}
}

func TestMentionsUser(t *testing.T) {
	tests := []struct {
		name string
		text string
		want bool
	}{
		{"plain mention", "<@U0BOT> hello", true},
		{"mention with a display label", "<@U0BOT|bridge> hello", true},
		{"mention in the middle", "hey <@U0BOT> can you", true},
		{"someone else", "<@U0OTHER> hello", false},
		{"the id without the markup", "U0BOT is the bot", false},
		{"a longer id that starts the same", "<@U0BOTTOM> hello", false},
		{"nothing at all", "hello", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mentionsUser(tt.text, testBotUser); got != tt.want {
				t.Errorf("mentionsUser(%q) = %v, want %v", tt.text, got, tt.want)
			}
		})
	}

	if mentionsUser("<@> hello", "") {
		t.Error("mentionsUser matched something with no user ID to match")
	}
}
