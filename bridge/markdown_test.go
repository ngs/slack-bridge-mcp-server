package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/slack-go/slack"
)

// slackCall is one request the client made, reduced to the fields that decide
// how the message renders.
type slackCall struct {
	Path     string
	Text     string
	ThreadTS string
	Blocks   []map[string]any
}

// fakeSlack stands in for the Web API so the outgoing payload can be read
// back. replies are returned in order, the last one repeating.
type fakeSlack struct {
	server  *httptest.Server
	calls   []slackCall
	replies []string
}

func newFakeSlack(t *testing.T, replies ...string) (*fakeSlack, *webAPI) {
	t.Helper()
	if len(replies) == 0 {
		replies = []string{`{"ok":true,"channel":"C1","ts":"100.000900"}`}
	}

	f := &fakeSlack{replies: replies}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parsing the request form: %v", err)
		}

		call := slackCall{
			Path:     r.URL.Path,
			Text:     r.PostFormValue("text"),
			ThreadTS: r.PostFormValue("thread_ts"),
		}
		if raw := r.PostFormValue("blocks"); raw != "" && raw != "[]" {
			if err := json.Unmarshal([]byte(raw), &call.Blocks); err != nil {
				t.Errorf("decoding the blocks payload %q: %v", raw, err)
			}
		}
		f.calls = append(f.calls, call)

		reply := f.replies[min(len(f.calls)-1, len(f.replies)-1)]
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(f.server.Close)

	return f, &webAPI{client: slack.New("xoxb-test", slack.OptionAPIURL(f.server.URL+"/"))}
}

func (f *fakeSlack) last() slackCall {
	if len(f.calls) == 0 {
		return slackCall{}
	}
	return f.calls[len(f.calls)-1]
}

func blockTypes(call slackCall) []string {
	types := make([]string, 0, len(call.Blocks))
	for _, b := range call.Blocks {
		types = append(types, b["type"].(string))
	}
	return types
}

func TestPostSendsAMarkdownBlock(t *testing.T) {
	f, api := newFakeSlack(t)

	text := "## Status\n\n**done** — see [the PR](https://example.com/pull/1)"
	ts, err := api.Post(context.Background(), "C1", "", text)
	if err != nil {
		t.Fatalf("Post() error = %v", err)
	}
	if ts != "100.000900" {
		t.Errorf("Post() = %q, want the posted ts", ts)
	}

	call := f.last()
	if got := blockTypes(call); len(got) != 1 || got[0] != "markdown" {
		t.Fatalf("blocks = %v, want a single markdown block", got)
	}
	if got := call.Blocks[0]["text"]; got != text {
		t.Errorf("block text = %q, want the Markdown passed through untouched", got)
	}
	// Without this the owner's phone shows an empty push notification.
	if call.Text != text {
		t.Errorf("fallback text = %q, want the message text", call.Text)
	}
}

func TestPostSendsLongTextAsPlainText(t *testing.T) {
	f, api := newFakeSlack(t)

	// One character past what a markdown block holds, so the block cannot
	// carry it and splitting would not help: the budget is per message.
	text := strings.Repeat("a", maxMarkdownBlock+1)
	if _, err := api.Post(context.Background(), "C1", "", text); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	call := f.last()
	if len(call.Blocks) != 0 {
		t.Errorf("blocks = %v, want none for text over the budget", blockTypes(call))
	}
	if call.Text != text {
		t.Error("the message text was not sent whole")
	}
	if len(f.calls) != 1 {
		t.Errorf("made %d calls, want one: the size is known before sending", len(f.calls))
	}
}

// Slack counts the budget after expanding what it finds inside the block, so a
// message can be within the character count and still be turned away. The
// answer still has to reach the owner.
func TestPostFallsBackToPlainTextWhenSlackRejectsTheBlock(t *testing.T) {
	for _, slackError := range []string{"msg_too_long", "internal_error"} {
		t.Run(slackError, func(t *testing.T) {
			f, api := newFakeSlack(t,
				`{"ok":false,"error":"`+slackError+`"}`,
				`{"ok":true,"channel":"C1","ts":"100.000900"}`,
			)

			text := strings.Repeat("😀", 1500)
			ts, err := api.Post(context.Background(), "C1", "", text)
			if err != nil {
				t.Fatalf("Post() error = %v, want the fallback to have carried it", err)
			}
			if ts != "100.000900" {
				t.Errorf("Post() = %q, want the ts of the message that landed", ts)
			}

			if len(f.calls) != 2 {
				t.Fatalf("made %d calls, want the block attempt and the plain-text retry", len(f.calls))
			}
			if got := blockTypes(f.calls[0]); len(got) != 1 || got[0] != "markdown" {
				t.Errorf("first attempt blocks = %v, want the markdown block", got)
			}
			if len(f.calls[1].Blocks) != 0 {
				t.Errorf("retry blocks = %v, want plain text", blockTypes(f.calls[1]))
			}
			if f.calls[1].Text != text {
				t.Error("the retry did not carry the whole message")
			}
		})
	}
}

func TestPostDoesNotRetryOtherFailures(t *testing.T) {
	f, api := newFakeSlack(t, `{"ok":false,"error":"channel_not_found"}`)

	if _, err := api.Post(context.Background(), "C1", "", "hello"); err == nil {
		t.Fatal("Post() = nil error, want the Slack error reported")
	}
	if len(f.calls) != 1 {
		t.Errorf("made %d calls, want no retry for an error the fallback cannot fix", len(f.calls))
	}
}

// The blocks must not cost the reply its place in the conversation.
func TestPostKeepsThreadingAlongsideTheBlock(t *testing.T) {
	f, api := newFakeSlack(t)

	if _, err := api.Post(context.Background(), "C1", "100.000100", "**hello**"); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	call := f.last()
	if call.ThreadTS != "100.000100" {
		t.Errorf("thread_ts = %q, want the thread the reply answers", call.ThreadTS)
	}
	if got := blockTypes(call); len(got) != 1 || got[0] != "markdown" {
		t.Errorf("blocks = %v, want the markdown block", got)
	}
}

func TestPostQuestionComposesMarkdownWithTheButtons(t *testing.T) {
	f, api := newFakeSlack(t)

	q := Question{
		BlockID: askBlockID,
		Text:    "**Deploy** now?",
		Options: []QuestionOption{
			{ActionID: askActionPrefix + "0", Value: "0", Label: "yes"},
			{ActionID: askActionPrefix + "1", Value: "1", Label: "no"},
		},
	}
	if _, err := api.PostQuestion(context.Background(), "C1", "", q); err != nil {
		t.Fatalf("PostQuestion() error = %v", err)
	}

	call := f.last()
	if got := blockTypes(call); len(got) != 2 || got[0] != "markdown" || got[1] != "actions" {
		t.Fatalf("blocks = %v, want the question as markdown followed by the buttons", got)
	}
	if got := call.Blocks[0]["text"]; got != q.Text {
		t.Errorf("question text = %q, want the Markdown passed through", got)
	}
	if call.Text != q.Text {
		t.Errorf("fallback text = %q, want the question", call.Text)
	}
}

// The retired question has to lose its buttons — they would still be clickable
// — while keeping the rendering the live one had.
func TestResolveQuestionReplacesTheButtonsWithTheMarkdown(t *testing.T) {
	f, api := newFakeSlack(t, `{"ok":true,"channel":"C1","ts":"100.000900"}`)

	text := "**Deploy** now?\n\n✅ yes"
	if err := api.ResolveQuestion(context.Background(), "C1", "100.000900", text); err != nil {
		t.Fatalf("ResolveQuestion() error = %v", err)
	}

	call := f.last()
	if !strings.Contains(call.Path, "chat.update") {
		t.Errorf("called %q, want chat.update", call.Path)
	}
	if got := blockTypes(call); len(got) != 1 || got[0] != "markdown" {
		t.Fatalf("blocks = %v, want the markdown block alone, with no actions left", got)
	}
	if got := call.Blocks[0]["text"]; got != text {
		t.Errorf("block text = %q, want the resolved question", got)
	}
}

func TestFitsMarkdownBlock(t *testing.T) {
	tests := []struct {
		name string
		text string
		want bool
	}{
		{"empty", "", true},
		{"short", "hello", true},
		{"at the budget", strings.Repeat("a", maxMarkdownBlock), true},
		{"one over", strings.Repeat("a", maxMarkdownBlock+1), false},
		// Counted in code points, not bytes: these are three bytes each, and
		// Slack accepts 12,000 of them.
		{"cjk at the budget", strings.Repeat("あ", maxMarkdownBlock), true},
		{"cjk one over", strings.Repeat("あ", maxMarkdownBlock+1), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := fitsMarkdownBlock(tc.text); got != tc.want {
				t.Errorf("fitsMarkdownBlock(%d chars) = %v, want %v", len([]rune(tc.text)), got, tc.want)
			}
		})
	}
}

func TestRejectedForSize(t *testing.T) {
	tests := []struct {
		name string
		err  error
		text string
		want bool
	}{
		{"nil", nil, "hello", false},
		{"too long", slack.SlackErrorResponse{Err: "msg_too_long"}, "hello", true},
		{"wrapped", fmt.Errorf("chat.postMessage: %w", slack.SlackErrorResponse{Err: "msg_too_long"}), "hello", true},
		{"unrelated", slack.SlackErrorResponse{Err: "channel_not_found"}, "hello", false},
		// internal_error is Slack's generic failure, so it only counts as a
		// size rejection where expansion could plausibly have caused it.
		// Otherwise a retry could repeat a post Slack already accepted.
		{"internal on plain text", slack.SlackErrorResponse{Err: "internal_error"}, "done", false},
		{"internal on cjk text", slack.SlackErrorResponse{Err: "internal_error"}, "完了しました", false},
		{"internal on a couple of emoji", slack.SlackErrorResponse{Err: "internal_error"}, "done 🎉", false},
		// Every resolved question carries one of these markers, and a status
		// marker is not why a short message would be refused.
		{"internal on a resolved question", slack.SlackErrorResponse{Err: "internal_error"}, "Deploy now?\n\n✅ `yes`", false},
		{"internal on a wall of emoji", slack.SlackErrorResponse{Err: "internal_error"}, strings.Repeat("🎉", 1500), true},
		// Nearly full with a single emoji: whether its shortcode tips the
		// message over depends on which emoji it is, so this has to be
		// retryable or a reply that Slack refused would simply be lost.
		{"internal on a nearly full message with one emoji", slack.SlackErrorResponse{Err: "internal_error"}, strings.Repeat("a", maxMarkdownBlock-10) + "🙂", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := rejectedForSize(tc.err, tc.text); got != tc.want {
				t.Errorf("rejectedForSize(%v, %q) = %v, want %v", tc.err, tc.text, got, tc.want)
			}
		})
	}
}

func TestLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "yes", "`yes`"},
		{"bold", "**yes**", "`**yes**`"},
		{"underscores", "run__it", "`run__it`"},
		{"link", "[docs](https://example.com)", "`[docs](https://example.com)`"},
		{"autolink", "<https://example.com>", "`<https://example.com>`"},
		{"entity", "a & b", "`a & b`"},
		{"emoji shortcode", ":shipit:", "`:shipit:`"},
		{"cjk", "はい", "`はい`"},
		// A label with backticks needs a longer fence, and one at either end
		// needs padding so it does not merge into it.
		{"contains a backtick", "go `test`", "`` go `test` ``"},
		{"starts with a backtick", "`x", "`` `x ``"},
		{"double run inside", "a ``b`` c", "```a ``b`` c```"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := literal(tc.in); got != tc.want {
				t.Errorf("literal(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// The indicator is furniture the server rewrites in place, and chat.update
// sends only text: blocks on the original post would freeze the stopwatch at
// whatever it said when it was created.
func TestPostPlainSendsNoBlocks(t *testing.T) {
	f, api := newFakeSlack(t)

	if _, err := api.PostPlain(context.Background(), "C1", "", "⏳ Working… (10s)"); err != nil {
		t.Fatalf("PostPlain() error = %v", err)
	}

	call := f.last()
	if len(call.Blocks) != 0 {
		t.Errorf("blocks = %v, want none so a text-only update replaces what is shown", blockTypes(call))
	}
	if call.Text != "⏳ Working… (10s)" {
		t.Errorf("text = %q, want the indicator line", call.Text)
	}
}

// Losing the question altogether would leave slack_ask with nothing to return,
// so a size rejection keeps the buttons and gives up only the block.
func TestPostQuestionFallsBackButKeepsTheButtons(t *testing.T) {
	f, api := newFakeSlack(t,
		`{"ok":false,"error":"msg_too_long"}`,
		`{"ok":true,"channel":"C1","ts":"100.000900"}`,
	)

	q := Question{
		BlockID: askBlockID,
		Text:    "**Deploy** now? 🎉",
		Options: []QuestionOption{
			{ActionID: askActionPrefix + "0", Value: "0", Label: "yes"},
			{ActionID: askActionPrefix + "1", Value: "1", Label: "no"},
		},
	}
	if _, err := api.PostQuestion(context.Background(), "C1", "", q); err != nil {
		t.Fatalf("PostQuestion() error = %v, want the fallback to have carried it", err)
	}

	if len(f.calls) != 2 {
		t.Fatalf("made %d calls, want the block attempt and the fallback", len(f.calls))
	}
	if got := blockTypes(f.calls[1]); len(got) != 2 || got[0] != "section" || got[1] != "actions" {
		t.Errorf("fallback blocks = %v, want a section question with the buttons still under it", got)
	}
}

// A question that was living in a section block, because Slack would not
// render it, has to retire the same way rather than dropping to bare text.
func TestResolveQuestionFallsBackToASectionFirst(t *testing.T) {
	f, api := newFakeSlack(t,
		`{"ok":false,"error":"msg_too_long"}`,
		`{"ok":true,"channel":"C1","ts":"100.000900"}`,
	)

	if err := api.ResolveQuestion(context.Background(), "C1", "100.000900", "**Deploy**? 🎉\n\n✅ `yes`"); err != nil {
		t.Fatalf("ResolveQuestion() error = %v", err)
	}

	if len(f.calls) != 2 {
		t.Fatalf("made %d calls, want the block attempt and the section retry", len(f.calls))
	}
	if got := blockTypes(f.calls[1]); len(got) != 1 || got[0] != "section" {
		t.Errorf("retry blocks = %v, want a section block, which still leaves no actions block", got)
	}
}

// The buttons have to go even when nothing renders, or an answered question
// stays answerable.
func TestResolveQuestionRetiresTheButtonsEvenWhenNothingRenders(t *testing.T) {
	f, api := newFakeSlack(t,
		`{"ok":false,"error":"msg_too_long"}`,
		`{"ok":false,"error":"msg_too_long"}`,
		`{"ok":true,"channel":"C1","ts":"100.000900"}`,
	)

	if err := api.ResolveQuestion(context.Background(), "C1", "100.000900", "**Deploy**? 🎉\n\n✅ `yes`"); err != nil {
		t.Fatalf("ResolveQuestion() error = %v", err)
	}

	if len(f.calls) != 3 {
		t.Fatalf("made %d calls, want markdown, then section, then no blocks at all", len(f.calls))
	}
	if len(f.calls[2].Blocks) != 0 {
		t.Errorf("last blocks = %v, want an empty list so the actions block is dropped", blockTypes(f.calls[2]))
	}
}

// buildQuestion must hand the caller's Markdown through untouched: the block
// tests above call PostQuestion directly and would not notice it being altered
// on the way in.
func TestBuildQuestionKeepsTheMarkdown(t *testing.T) {
	text := "## Deploy\n\n**now**? See [the plan](https://example.com)."
	q, labels, err := buildQuestion(text, []string{"**yes**", "no"})
	if err != nil {
		t.Fatalf("buildQuestion() error = %v", err)
	}
	if q.Text != text {
		t.Errorf("question text = %q, want the Markdown unchanged", q.Text)
	}
	// Labels ride on buttons, which render plain text.
	if labels[0] != "**yes**" {
		t.Errorf("option label = %q, want it left alone", labels[0])
	}
}

func TestBuildQuestionTruncatesAtTheCap(t *testing.T) {
	atCap := strings.Repeat("あ", maxQuestionText)
	q, _, err := buildQuestion(atCap, []string{"yes", "no"})
	if err != nil {
		t.Fatalf("buildQuestion() error = %v", err)
	}
	if q.Text != atCap {
		t.Error("a question exactly at the cap was altered")
	}

	q, _, err = buildQuestion(atCap+"あ", []string{"yes", "no"})
	if err != nil {
		t.Fatalf("buildQuestion() error = %v", err)
	}
	if got := len([]rune(q.Text)); got != maxQuestionText {
		t.Errorf("question ran to %d characters, want it cut to %d", got, maxQuestionText)
	}
	if !strings.HasSuffix(q.Text, "…") {
		t.Error("a cut question does not say it was cut")
	}
}

// The answer is quoted back into a markdown block, but the owner chose it on a
// plain_text button.
func TestAnsweredTextQuotesTheChosenLabel(t *testing.T) {
	got := answeredText("Deploy **now**?", "**yes**")
	want := "Deploy **now**?\n\n✅ `**yes**`"
	if got != want {
		t.Errorf("answeredText() = %q, want %q", got, want)
	}
}

// The tests above call webAPI directly, so nothing yet stops the bridge from
// altering the text on its way there — which is exactly what it used to do,
// and what the markdown block makes wrong.
func TestBridgePostHandsTheMarkdownOverUntouched(t *testing.T) {
	api := &fakeAPI{postTS: "100.000900"}
	b := New(context.Background(), testConfig(t), &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	text := "## Status\n\n**done** — see [the PR](https://example.com/pull/1)\n\n| a | b |\n|---|---|\n| 1 | 2 |"
	if _, err := b.Post(context.Background(), PostRequest{Text: text}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	posts := agentPosts(api)
	if len(posts) != 1 {
		t.Fatalf("agent posts = %d, want one", len(posts))
	}
	if posts[0].Text != text {
		t.Errorf("the bridge changed the text on the way out:\n got %q\nwant %q", posts[0].Text, text)
	}
}

// A resolved question can outgrow a section block even though the question
// itself was capped, because the marker and the answer are added to it. The
// step that cannot succeed is skipped rather than spent.
func TestResolveQuestionSkipsTheSectionWhenItCannotFit(t *testing.T) {
	f, api := newFakeSlack(t,
		`{"ok":false,"error":"msg_too_long"}`,
		`{"ok":true,"channel":"C1","ts":"100.000900"}`,
	)

	text := strings.Repeat("a", maxSectionBlock+1)
	if err := api.ResolveQuestion(context.Background(), "C1", "100.000900", text); err != nil {
		t.Fatalf("ResolveQuestion() error = %v", err)
	}

	if len(f.calls) != 2 {
		t.Fatalf("made %d calls, want the block attempt and then no blocks at all", len(f.calls))
	}
	if len(f.calls[1].Blocks) != 0 {
		t.Errorf("second attempt blocks = %v, want the empty list that drops the buttons", blockTypes(f.calls[1]))
	}
}

// A label showing on two lines on the button and on one in the resolution
// would stop matching what the owner clicked, so it is one line from the
// start.
func TestBuildQuestionFlattensMultilineLabels(t *testing.T) {
	_, labels, err := buildQuestion("Deploy?", []string{"yes\nplease", "no"})
	if err != nil {
		t.Fatalf("buildQuestion() error = %v", err)
	}
	if labels[0] != "yes please" {
		t.Errorf("option label = %q, want the line break flattened to a space", labels[0])
	}
	if got := answeredText("Deploy?", labels[0]); !strings.Contains(got, "`yes please`") {
		t.Errorf("answeredText() = %q, want it to quote the same one-line label", got)
	}
}
