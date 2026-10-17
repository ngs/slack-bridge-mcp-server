package bridge

import (
	"context"
	"errors"
	"fmt"
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
