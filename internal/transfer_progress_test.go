package internal_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/progress"
	"github.com/wal-g/wal-g/pkg/storages/fs"
)

// A real filesystem transfer must reach the command's tracker, including
// uploads through a cloned uploader and reads before the object is complete.
func TestBackupTransferProgress(t *testing.T) {
	for _, direction := range []string{"upload", "download"} {
		t.Run(direction, func(t *testing.T) {
			folder := fs.NewFolder(t.TempDir(), "")
			payload := strings.Repeat("x", 2048)
			ctx := context.Background()
			if direction == "download" {
				require.NoError(t, folder.PutObject(ctx, "part", strings.NewReader(payload)))
			}
			frames := make(progressFrames, 32)
			tracker := progress.Start(ctx, frames, direction, 120)
			defer tracker.Stop()
			ctx = progress.WithTracker(ctx, tracker)
			if direction == "upload" {
				uploader := internal.NewRegularUploader(nil, folder).Clone()
				require.NoError(t, uploader.Upload(ctx, "part", strings.NewReader(payload)))
				uploader.Finish()
			} else {
				reader, err := internal.NewStorageReaderMaker(folder, "part").Reader(ctx)
				require.NoError(t, err)
				// Count live reads rather than only completed archives.
				_, err = io.CopyN(io.Discard, reader, 1024)
				require.NoError(t, err)
				awaitProgress(t, frames, "1.0 KiB")
				_, err = io.Copy(io.Discard, reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
			}
			awaitProgress(t, frames, "2.0 KiB")

		})
	}
}

type progressFrames chan string

func (f progressFrames) Write(b []byte) (int, error) {
	select {
	case f <- string(b):
	default:
	}
	return len(b), nil
}

func awaitProgress(t *testing.T, frames progressFrames, want string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case frame := <-frames:
			if strings.Contains(frame, want) {
				return
			}
		case <-timer.C:
			t.Fatalf("transfer did not report %s", want)
		}
	}
}
