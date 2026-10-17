package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

// MaxUploadBytes is the largest file slack_upload will send. Slack's own limit
// for an external upload is 1 GB; checking it here turns an oversized file
// into a clear error before any bytes leave the machine.
const MaxUploadBytes int64 = 1 << 30

// UploadRequest is a local file to share and where to share it.
type UploadRequest struct {
	Path string
	// Channel is where the file goes. Empty means the home channel, as with
	// every other tool.
	Channel        string
	ThreadTS       string
	InitialComment string
	// Filename is the name Slack shows; empty means the file's own base name.
	Filename string
	// Title is the label above the file; empty leaves it to Slack, which uses
	// the file name.
	Title string
}

// UploadResult reports the shared file and where it went.
type UploadResult struct {
	FileID  string `json:"file_id"`
	Title   string `json:"title,omitempty"`
	Channel string `json:"channel"`
}

// Upload shares a local file into a conversation, optionally inside a thread
// and with a comment, through Slack's external upload: ask for an upload URL,
// send the bytes there, then complete the upload into the channel.
//
// It treats the upload as a reply, the way Post does: the processing indicator
// is retired before the file lands and put back if the upload fails.
func (b *Bridge) Upload(ctx context.Context, req UploadRequest) (UploadResult, error) {
	// Everything that can be checked locally is checked before connecting, so
	// a wrong path is answered without opening a socket.
	if req.Path == "" {
		return UploadResult{}, errors.New("path is required")
	}
	info, err := os.Stat(req.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return UploadResult{}, fmt.Errorf("no file at %s", req.Path)
		}
		return UploadResult{}, fmt.Errorf("reading %s: %w", req.Path, err)
	}
	if info.IsDir() {
		return UploadResult{}, fmt.Errorf("%s is a directory; pass a file", req.Path)
	}
	if !info.Mode().IsRegular() {
		return UploadResult{}, fmt.Errorf("%s is not a regular file", req.Path)
	}
	if info.Size() == 0 {
		return UploadResult{}, fmt.Errorf("%s is empty; Slack does not accept an empty upload", req.Path)
	}
	if info.Size() > MaxUploadBytes {
		return UploadResult{}, fmt.Errorf("%s is %d bytes, over the %d byte limit for an upload", req.Path, info.Size(), MaxUploadBytes)
	}

	filename := req.Filename
	if filename == "" {
		filename = filepath.Base(req.Path)
	}

	retired := b.stopIndicator()

	api, _, err := b.apiForCall()
	if err != nil {
		b.restoreIndicator(ctx, retired)
		return UploadResult{}, err
	}
	uploader, ok := api.(FileUploader)
	if !ok {
		b.restoreIndicator(ctx, retired)
		return UploadResult{}, errors.New("this Slack connection cannot upload files")
	}

	channel := b.channelFor(req.Channel)
	file, err := uploader.UploadFile(ctx, FileUpload{
		Path:           req.Path,
		Size:           info.Size(),
		Channel:        channel,
		ThreadTS:       req.ThreadTS,
		InitialComment: req.InitialComment,
		Filename:       filename,
		Title:          req.Title,
	})
	if err != nil {
		b.restoreIndicator(ctx, retired)
		if errors.Is(err, ErrMissingScope) {
			return UploadResult{}, fmt.Errorf("%w: uploading needs the files:write bot scope; add it to the app and reinstall it", err)
		}
		return UploadResult{}, err
	}
	return UploadResult{FileID: file.ID, Title: file.Title, Channel: channel}, nil
}

// errFileShrank marks a file that had fewer bytes than its checked size by the
// time it was sent.
var errFileShrank = errors.New("the file shrank after its size was checked")

// sendFile POSTs exactly size bytes of r to a Slack upload URL as the "file"
// field of a multipart form.
//
// slack-go's UploadToURL is not used for this, for two reasons. It copies the
// reader through EOF, so a file that grows after its size was checked — a log
// still being written — sends more than the length the URL was issued for,
// and more than MaxUploadBytes allows. And its writer goroutine reports on an
// unbuffered channel that nobody reads once the request fails or is
// cancelled, so every failed upload leaves a goroutine blocked forever.
//
// Here the body is bounded to size, a file that shrank is an error rather than
// a short upload, and the function does not return until the writer has
// finished: closing the pipe with the request's error unblocks it, and its
// result goes to a buffered channel.
func sendFile(ctx context.Context, client *http.Client, uploadURL, token, filename string, r io.Reader, size int64) error {
	if client == nil {
		client = http.DefaultClient
	}

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	done := make(chan error, 1)
	go func() {
		err := func() error {
			part, err := mw.CreateFormFile("file", filename)
			if err != nil {
				return err
			}
			n, err := io.Copy(part, io.LimitReader(r, size))
			if err != nil {
				return err
			}
			if n != size {
				return fmt.Errorf("%w: %d bytes after it was checked at %d", errFileShrank, n, size)
			}
			return mw.Close()
		}()
		// A nil error closes the pipe with EOF, which ends the request body.
		_ = pw.CloseWithError(err)
		done <- err
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, pr)
	if err != nil {
		_ = pr.CloseWithError(err)
		<-done
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		_ = pr.CloseWithError(err)
		// A file that shrank is the cause when it happened; any other writer
		// error is only the closed pipe, an echo of the request's own.
		if writeErr := <-done; errors.Is(writeErr, errFileShrank) {
			return writeErr
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	// A server that answered before reading the whole body leaves the writer
	// blocked on the pipe; closing it lets the writer finish with an error,
	// which is reported, since then not every byte was taken.
	_ = pr.CloseWithError(errors.New("the upload URL answered before the file was sent"))
	writeErr := <-done
	// A file that shrank also makes the server reject the body, so it is
	// checked first: it says why.
	if errors.Is(writeErr, errFileShrank) {
		return writeErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("the upload URL answered %s", resp.Status)
	}
	return writeErr
}
