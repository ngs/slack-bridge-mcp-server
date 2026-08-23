package bridge

import (
	"context"
	"strings"
	"testing"
)

func TestNormalizeMrkdwnBold(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"double asterisk", "**bold**", "*bold*"},
		{"double underscore", "__bold__", "*bold*"},
		{"single asterisk left alone", "*already*", "*already*"},
		{"single underscore left alone", "_italic_", "_italic_"},
		{"bold inside a sentence", "the **build** passed", "the *build* passed"},
		{"punctuation right after", "it is **done**, finally", "it is *done*, finally"},
		{"punctuation right before", "(**done**)", "(*done*)"},
		{"two spans on one line", "**a** and **b**", "*a* and *b*"},
		{"triple asterisk left alone", "***both***", "***both***"},
		{"triple underscore left alone", "___both___", "___both___"},
		{"empty span left alone", "****", "****"},
		{"padded content left alone", "** not bold **", "** not bold **"},
		{"unclosed span left alone", "**dangling", "**dangling"},
		{"multiplication left alone", "2 ** 3 ** 4", "2 ** 3 ** 4"},
		{"inner asterisk left alone", "**a*b**", "**a*b**"},
		{"snake case left alone", "dunder__name__thing", "dunder__name__thing"},
		{"underscore after word left alone", "call__init__", "call__init__"},
		{"cjk neighbours still convert", "太字__強調__です", "太字*強調*です"},
		{"cjk with asterisks", "**確認しました**、ありがとうございます", "*確認しました*、ありがとうございます"},
		{"escaped asterisks left alone", `\*\*literal\*\*`, `\*\*literal\*\*`},
		{"multiline", "**one**\nplain\n__two__", "*one*\nplain\n*two*"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMrkdwn(tc.in); got != tc.want {
				t.Errorf("normalizeMrkdwn(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeMrkdwnCode(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"inline span untouched",
			"run `make **all**` first",
			"run `make **all**` first",
		},
		{
			"bold outside a span still converts",
			"**run** `make **all**`",
			"*run* `make **all**`",
		},
		{
			"link inside a span untouched",
			"see `[label](https://example.com)`",
			"see `[label](https://example.com)`",
		},
		{
			"double backtick span untouched",
			"``a ` **b**`` and **c**",
			"``a ` **b**`` and *c*",
		},
		{
			"stray backtick does not swallow the line",
			"a ` b **c**",
			"a ` b *c*",
		},
		{
			"bold must not span into a code span",
			"start ** and `**x**`",
			"start ** and `**x**`",
		},
		{
			"closing delimiter inside a span does not close",
			"start **and `**x**`",
			"start **and `**x**`",
		},
		{
			"even length span inside a bold candidate does not close it",
			"**a ``**x**`` b**",
			"**a ``**x**`` b**",
		},
		{
			"bold around a whole code span still converts",
			"**a `x` b**",
			"*a `x` b*",
		},
		{
			"fenced block untouched",
			"before **x**\n```\n**y**\n# not a heading\n```\nafter **z**",
			"before *x*\n```\n**y**\n# not a heading\n```\nafter *z*",
		},
		{
			"fenced block with a language tag",
			"```go\nconst a = 1 // **b**\n```",
			"```go\nconst a = 1 // **b**\n```",
		},
		{
			"indented fence still opens a block",
			"  ```\n  **x**\n  ```",
			"  ```\n  **x**\n  ```",
		},
		{
			"a longer fence contains shorter delimiter lines",
			"````\n```\n**x**\n```\n````\n**y**",
			"````\n```\n**x**\n```\n````\n*y*",
		},
		{
			"a shorter fence is closed by a longer delimiter",
			"```\n**x**\n`````\n**y**",
			"```\n**x**\n`````\n*y*",
		},
		{
			"unclosed fence protects the rest",
			"**a**\n```\n**b**\n**c**",
			"*a*\n```\n**b**\n**c**",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMrkdwn(tc.in); got != tc.want {
				t.Errorf("normalizeMrkdwn(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeMrkdwnLinks(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"plain link",
			"see [the PR](https://example.com/pull/1)",
			"see <https://example.com/pull/1|the PR>",
		},
		{
			"url containing parens",
			"[Go](https://en.wikipedia.org/wiki/Go_(language))",
			"<https://en.wikipedia.org/wiki/Go_(language)|Go>",
		},
		{
			"label containing parens",
			"[the PR (draft)](https://example.com)",
			"<https://example.com|the PR (draft)>",
		},
		{
			"bold label converts inside the link",
			"[**urgent**](https://example.com)",
			"<https://example.com|*urgent*>",
		},
		{
			"two links on one line",
			"[a](https://a.example) and [b](https://b.example)",
			"<https://a.example|a> and <https://b.example|b>",
		},
		{
			"cjk label",
			"[議事録はこちら](https://example.com/notes)",
			"<https://example.com/notes|議事録はこちら>",
		},
		{
			"image left alone",
			"![alt](https://example.com/a.png)",
			"![alt](https://example.com/a.png)",
		},
		{
			"already slack syntax left alone",
			"<https://example.com|the PR>",
			"<https://example.com|the PR>",
		},
		{
			"bare brackets left alone",
			"[WIP] still working",
			"[WIP] still working",
		},
		{
			"reference style left alone",
			"[label][ref]",
			"[label][ref]",
		},
		{
			"empty url left alone",
			"[label]()",
			"[label]()",
		},
		{
			"empty label left alone",
			"[](https://example.com)",
			"[](https://example.com)",
		},
		{
			"unbalanced parens left alone",
			"[label](https://example.com",
			"[label](https://example.com",
		},
		{
			"url with a space left alone",
			"[label](https://example.com/a b)",
			"[label](https://example.com/a b)",
		},
		{
			"url with a pipe left alone",
			"[label](https://example.com/a|b)",
			"[label](https://example.com/a|b)",
		},
		{
			"non-http target still converts",
			"[home](mailto:someone@example.com)",
			"<mailto:someone@example.com|home>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMrkdwn(tc.in); got != tc.want {
				t.Errorf("normalizeMrkdwn(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeMrkdwnHeadings(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"h1", "# Release", "*Release*"},
		{"h2", "## Release", "*Release*"},
		{"h3", "### Release", "*Release*"},
		{"h4 left alone", "#### Release", "#### Release"},
		{"trailing space trimmed", "##   Release   ", "*Release*"},
		{"indented heading", "  ## Release", "*Release*"},
		{"no space after hash left alone", "#release", "#release"},
		{"issue reference left alone", "fixes #12", "fixes #12"},
		{"hash mid line left alone", "the # sign", "the # sign"},
		{"heading later in the message", "intro\n\n## Status\ndone", "intro\n\n*Status*\ndone"},
		{"heading mid line left alone", "see ## below", "see ## below"},
		{"already bold heading is not double wrapped", "## **Status**", "*Status*"},
		{"heading with a link is not wrapped twice", "## [PR](https://example.com)", "*<https://example.com|PR>*"},
		{"heading with a partial bold keeps its own emphasis", "## Status: **red**", "Status: *red*"},
		{"bare hashes left alone", "###", "###"},
		{"cjk heading", "## 今日の予定", "*今日の予定*"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMrkdwn(tc.in); got != tc.want {
				t.Errorf("normalizeMrkdwn(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestNormalizeMrkdwnLeavesEverythingElseAlone pins the boundary of the
// feature: the conversion is bold, links and headings, and nothing else.
func TestNormalizeMrkdwnLeavesEverythingElseAlone(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"plain text", "the build passed"},
		{"bullet list", "- one\n- two"},
		{"numbered list", "1. one\n2. two"},
		{"blockquote", "> quoted"},
		{"strikethrough", "~~gone~~"},
		{"html", "<b>bold</b>"},
		{"slack mention", "<@U123456> please look"},
		{"slack emoji", ":eyes: on it"},
		{"ampersand not escaped", "a & b < c"},
		{"table", "| a | b |\n| - | - |"},
		{"horizontal rule", "---"},
		{"cjk prose", "本日の作業は完了しました。明日はレビューから始めます。"},
		{"trailing newline preserved", "done\n"},
		{"crlf preserved", "one\r\ntwo"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMrkdwn(tc.in); got != tc.in {
				t.Errorf("normalizeMrkdwn(%q) = %q, want it unchanged", tc.in, got)
			}
		})
	}
}

func TestNormalizeMrkdwnRealisticMessage(t *testing.T) {
	in := "## Deploy status\n" +
		"**Done:** the release is out. See [the PR](https://example.com/pull/7).\n" +
		"Run `git log --oneline **HEAD**` to check.\n" +
		"```\n" +
		"**not bold**\n" +
		"```\n" +
		"__Next:__ 明日の朝にレビューします。"

	want := "*Deploy status*\n" +
		"*Done:* the release is out. See <https://example.com/pull/7|the PR>.\n" +
		"Run `git log --oneline **HEAD**` to check.\n" +
		"```\n" +
		"**not bold**\n" +
		"```\n" +
		"*Next:* 明日の朝にレビューします。"

	if got := normalizeMrkdwn(in); got != want {
		t.Errorf("normalizeMrkdwn() =\n%q\nwant\n%q", got, want)
	}
}

// The three tools that put agent-written text on the channel each have to go
// through the normalizer; these pin that wiring at the seam, one test per tool.

func TestPostNormalizesMarkdown(t *testing.T) {
	api := &fakeAPI{postTS: "100.000900"}
	b := New(context.Background(), testConfig(t), &fakeConnector{api: api, stream: newFakeStream()})
	defer func() { _ = b.Close() }()

	if _, err := b.Post(context.Background(), PostRequest{Text: "**done** — see [the PR](https://example.com/pull/1)"}); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	want := "*done* — see <https://example.com/pull/1|the PR>"
	if len(api.posts) != 1 || api.posts[0].Text != want {
		t.Errorf("Post() sent %+v, want the text normalized to %q", api.posts, want)
	}
}

func TestAskNormalizesTheQuestion(t *testing.T) {
	// The option labels ride on buttons, which render plain text, so they are
	// the half that must come through exactly as written.
	q, labels, err := buildQuestion("**Deploy** now?", []string{"**yes**", "no"})
	if err != nil {
		t.Fatalf("buildQuestion() error = %v", err)
	}
	if q.Text != "*Deploy* now?" {
		t.Errorf("question text = %q, want it normalized to %q", q.Text, "*Deploy* now?")
	}
	if labels[0] != "**yes**" {
		t.Errorf("option label = %q, want it left alone", labels[0])
	}
}

func TestProgressNormalizesTheLabel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, api, _ := indicatorBridge(ctx, t)
	waitForMessages(ctx, t, b)
	eventually(t, "the indicator to be posted", func() bool { return len(indicatorPosts(api)) == 1 })

	if _, err := b.Progress(ctx, ProgressRequest{Text: "**release chain**: waiting for CI"}); err != nil {
		t.Fatalf("Progress() error = %v", err)
	}

	eventually(t, "the normalized label to reach the indicator", func() bool {
		return strings.Contains(lastIndicatorUpdate(api), "*release chain*: waiting for CI")
	})
	if got := lastIndicatorUpdate(api); strings.Contains(got, "**release chain**") {
		t.Errorf("indicator text = %q, want the Markdown bold normalized away", got)
	}
}
