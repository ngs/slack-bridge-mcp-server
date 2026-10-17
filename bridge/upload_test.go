package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/slack-go/slack"
)

// uploadingAPI is a fakeAPI that can also upload, the way webAPI can.
type uploadingAPI struct {
	*fakeAPI
	mu      sync.Mutex
	uploads []FileUpload
	err     error
}

func (u *uploadingAPI) UploadFile(_ context.Context, f FileUpload) (UploadedFile, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.err != nil {
		return UploadedFile{}, u.err
	}
	u.uploads = append(u.uploads, f)
	return UploadedFile{ID: "F123", Title: f.Title}, nil
}

// apiConnector hands out any API, which fakeConnector cannot: its api field is
// the concrete fake. It counts connects so a test can see none happened.
type apiConnector struct {
	api   API
	mu    sync.Mutex
	calls int
}

func (c *apiConnector) Connect(context.Context, Config) (API, Stream, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.api, newFakeStream(), nil
}

func (c *apiConnector) connectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newUploadBridge(t *testing.T, api API) *Bridge {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := New(ctx, testConfig(t), &apiConnector{api: api})
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func writeTemp(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Leaving the channel out has to mean the home channel, as it does for every
// other tool, and the file's own name has to stand in for a missing filename.
func TestUploadDefaultsToTheHomeChannelAndTheFileName(t *testing.T) {
	api := &uploadingAPI{fakeAPI: &fakeAPI{}}
	b := newUploadBridge(t, api)
	path := writeTemp(t, "report.txt", "hello")

	got, err := b.Upload(context.Background(), UploadRequest{Path: path, ThreadTS: "1.2", InitialComment: "here it is"})
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if got.FileID != "F123" || got.Channel != testChannel {
		t.Errorf("Upload() = %+v, want file F123 in the home channel %s", got, testChannel)
	}
	if len(api.uploads) != 1 {
		t.Fatalf("UploadFile called %d times, want 1", len(api.uploads))
	}
	f := api.uploads[0]
	want := FileUpload{Path: path, Size: 5, Channel: testChannel, ThreadTS: "1.2", InitialComment: "here it is", Filename: "report.txt"}
	if f != want {
		t.Errorf("UploadFile(%+v), want %+v", f, want)
	}
}

func TestUploadPassesTheNamedChannelAndLabels(t *testing.T) {
	api := &uploadingAPI{fakeAPI: &fakeAPI{}}
	b := newUploadBridge(t, api)
	path := writeTemp(t, "a.png", "png")

	got, err := b.Upload(context.Background(), UploadRequest{Path: path, Channel: "C999", Filename: "shot.png", Title: "Screenshot"})
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if got.Channel != "C999" || got.Title != "Screenshot" {
		t.Errorf("Upload() = %+v, want channel C999 and the title passed through", got)
	}
	f := api.uploads[0]
	if f.Channel != "C999" || f.Filename != "shot.png" || f.Title != "Screenshot" {
		t.Errorf("UploadFile(%+v), want the channel, filename and title from the request", f)
	}
}

// Everything wrong with the path is answered locally, before any connection
// and without a call to Slack.
func TestUploadRejectsPathsThatAreNotAFileToSend(t *testing.T) {
	dir := t.TempDir()
	empty := writeTemp(t, "empty.txt", "")
	big := filepath.Join(dir, "big.bin")
	f, err := os.Create(big)
	if err != nil {
		t.Fatal(err)
	}
	// Sparse, so the test does not write a gigabyte.
	if err := f.Truncate(MaxUploadBytes + 1); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	cases := map[string]struct {
		path string
		want string
	}{
		"no path":   {"", "path is required"},
		"missing":   {filepath.Join(dir, "nope.txt"), "no file at"},
		"directory": {dir, "is a directory"},
		"empty":     {empty, "is empty"},
		"too large": {big, "over the"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			api := &uploadingAPI{fakeAPI: &fakeAPI{}}
			connector := &apiConnector{api: api}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b := New(ctx, testConfig(t), connector)
			defer func() { _ = b.Close() }()

			_, err := b.Upload(context.Background(), UploadRequest{Path: tc.path})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Upload(%q) error = %v, want one mentioning %q", tc.path, err, tc.want)
			}
			if len(api.uploads) != 0 || connector.connectCount() != 0 {
				t.Errorf("a bad path reached Slack: %d uploads, %d connects", len(api.uploads), connector.connectCount())
			}
		})
	}
}

// A connection that cannot upload says so instead of failing obscurely.
func TestUploadSaysSoWhenTheConnectionCannotUpload(t *testing.T) {
	b := newUploadBridge(t, &fakeAPI{})
	_, err := b.Upload(context.Background(), UploadRequest{Path: writeTemp(t, "a.txt", "x")})
	if err == nil || !strings.Contains(err.Error(), "cannot upload") {
		t.Fatalf("Upload() error = %v, want the connection to say it cannot upload", err)
	}
}

// A missing scope is the failure a fresh install will hit, and the fix is
// outside the session, so the error names the scope to add.
func TestUploadNamesTheScopeWhenItIsMissing(t *testing.T) {
	api := &uploadingAPI{fakeAPI: &fakeAPI{}, err: fmt.Errorf("files.getUploadURLExternal: %w", ErrMissingScope)}
	b := newUploadBridge(t, api)
	_, err := b.Upload(context.Background(), UploadRequest{Path: writeTemp(t, "a.txt", "x")})
	if !errors.Is(err, ErrMissingScope) || !strings.Contains(err.Error(), "files:write") {
		t.Fatalf("Upload() error = %v, want ErrMissingScope naming files:write", err)
	}
}

// fakeUploadServer plays the three Slack endpoints of an external upload and
// records what each was sent, in order.
type fakeUploadServer struct {
	mu        sync.Mutex
	calls     []string
	urlForm   map[string]string
	uploaded  string
	upName    string
	complete  map[string]string
	urlError  string
	doneError string
}

func (s *fakeUploadServer) handler(t *testing.T, base func() string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.calls = append(s.calls, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/files.getUploadURLExternal":
			_ = r.ParseForm()
			s.urlForm = map[string]string{"filename": r.Form.Get("filename"), "length": r.Form.Get("length")}
			if s.urlError != "" {
				_, _ = fmt.Fprintf(w, `{"ok":false,"error":%q}`, s.urlError)
				return
			}
			_, _ = fmt.Fprintf(w, `{"ok":true,"upload_url":%q,"file_id":"F777"}`, base()+"/upload")
		case "/upload":
			_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil {
				t.Errorf("upload Content-Type: %v", err)
				return
			}
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Errorf("reading the upload: %v", err)
					return
				}
				if p.FormName() == "file" {
					raw, _ := io.ReadAll(p)
					s.uploaded, s.upName = string(raw), p.FileName()
				}
			}
			_, _ = w.Write([]byte("OK"))
		case "/files.completeUploadExternal":
			_ = r.ParseForm()
			s.complete = map[string]string{
				"files":           r.Form.Get("files"),
				"channel_id":      r.Form.Get("channel_id"),
				"thread_ts":       r.Form.Get("thread_ts"),
				"initial_comment": r.Form.Get("initial_comment"),
			}
			if s.doneError != "" {
				_, _ = fmt.Fprintf(w, `{"ok":false,"error":%q}`, s.doneError)
				return
			}
			_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F777","title":"Report"}]}`))
		default:
			t.Errorf("unexpected request to %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
}

func startUploadServer(t *testing.T, s *fakeUploadServer) *webAPI {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(s.handler(t, func() string { return srv.URL }))
	t.Cleanup(srv.Close)
	return &webAPI{client: slack.New("xoxb-test", slack.OptionAPIURL(srv.URL+"/"))}
}

// The three calls go in order, each with what Slack needs: the exact length
// for the URL, the bytes to that URL, and the channel, thread and comment to
// complete it into.
func TestWebAPIUploadMakesTheThreeCallsInOrder(t *testing.T) {
	s := &fakeUploadServer{}
	w := startUploadServer(t, s)
	path := writeTemp(t, "r.txt", "report body")

	got, err := w.UploadFile(context.Background(), FileUpload{
		Path: path, Size: 11, Channel: "C1", ThreadTS: "10.5", InitialComment: "see attached", Filename: "report.txt", Title: "Report",
	})
	if err != nil {
		t.Fatalf("UploadFile() error = %v", err)
	}
	if got.ID != "F777" || got.Title != "Report" {
		t.Errorf("UploadFile() = %+v, want F777 titled Report", got)
	}
	wantCalls := []string{"/files.getUploadURLExternal", "/upload", "/files.completeUploadExternal"}
	if strings.Join(s.calls, ",") != strings.Join(wantCalls, ",") {
		t.Errorf("calls = %v, want %v", s.calls, wantCalls)
	}
	if s.urlForm["filename"] != "report.txt" || s.urlForm["length"] != "11" {
		t.Errorf("getUploadURLExternal got %v, want filename report.txt and length 11", s.urlForm)
	}
	if s.uploaded != "report body" || s.upName != "report.txt" {
		t.Errorf("uploaded %q as %q, want the file's bytes as report.txt", s.uploaded, s.upName)
	}
	if s.complete["channel_id"] != "C1" || s.complete["thread_ts"] != "10.5" || s.complete["initial_comment"] != "see attached" {
		t.Errorf("completeUploadExternal got %v, want the channel, thread and comment", s.complete)
	}
	if !strings.Contains(s.complete["files"], `"id":"F777"`) || !strings.Contains(s.complete["files"], `"title":"Report"`) {
		t.Errorf("completeUploadExternal files = %s, want the file id and title", s.complete["files"])
	}
}

// Without a title the file name is sent, rather than an empty title.
func TestWebAPIUploadTitlesTheFileByItsNameByDefault(t *testing.T) {
	s := &fakeUploadServer{}
	w := startUploadServer(t, s)
	if _, err := w.UploadFile(context.Background(), FileUpload{Path: writeTemp(t, "a.txt", "x"), Size: 1, Channel: "C1", Filename: "a.txt"}); err != nil {
		t.Fatalf("UploadFile() error = %v", err)
	}
	if !strings.Contains(s.complete["files"], `"title":"a.txt"`) {
		t.Errorf("completeUploadExternal files = %s, want the file name as the title", s.complete["files"])
	}
}

// A missing scope on the first call stops there, marked so the bridge can
// name the scope; nothing is sent.
func TestWebAPIUploadStopsOnAMissingScope(t *testing.T) {
	s := &fakeUploadServer{urlError: "missing_scope"}
	w := startUploadServer(t, s)
	_, err := w.UploadFile(context.Background(), FileUpload{Path: writeTemp(t, "a.txt", "x"), Size: 1, Channel: "C1", Filename: "a.txt"})
	if !errors.Is(err, ErrMissingScope) || !strings.Contains(err.Error(), "files.getUploadURLExternal") {
		t.Fatalf("UploadFile() error = %v, want ErrMissingScope from files.getUploadURLExternal", err)
	}
	if len(s.calls) != 1 {
		t.Errorf("calls = %v, want only the first", s.calls)
	}
}

// A failure to complete — the bot not in the channel, say — comes back with
// Slack's own reason in it.
func TestWebAPIUploadReportsAFailedCompletion(t *testing.T) {
	s := &fakeUploadServer{doneError: "not_in_channel"}
	w := startUploadServer(t, s)
	_, err := w.UploadFile(context.Background(), FileUpload{Path: writeTemp(t, "a.txt", "x"), Size: 1, Channel: "C1", Filename: "a.txt"})
	if err == nil || !strings.Contains(err.Error(), "files.completeUploadExternal") || !strings.Contains(err.Error(), "not_in_channel") {
		t.Fatalf("UploadFile() error = %v, want files.completeUploadExternal: not_in_channel", err)
	}
}
