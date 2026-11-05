package postgres

import (
	"context"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/progress"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestSelectedArchiveDownloadProgress(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		skip, legacy, noFiles bool
		sizes                 map[string]int64
		total                 string
	}{
		{name: "all", total: "3.2 KiB"},
		{name: "filtered", skip: true, total: "1.2 KiB"},
		{name: "legacy skips control and label", skip: true, legacy: true, total: "1.0 KiB"},
		{name: "no useful archives", skip: true, noFiles: true},
		{name: "unknown size", sizes: map[string]int64{"a.tar": -1}},
		{name: "zero size", sizes: map[string]int64{"a.tar": 0}},
		{name: "overflow", sizes: map[string]int64{"a.tar": math.MaxInt64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lists := 0
			folder := &progressListingFolder{Folder: fs.NewFolder(t.TempDir(), ""), lists: &lists, sizes: tc.sizes}
			name := "base_test"
			if tc.legacy {
				name = "base_000000010000DD170000000C_00743784"
			}
			backup := Backup{
				Backup:           internal.Backup{Name: name, Folder: folder},
				SentinelDto:      &BackupSentinelDto{},
				FilesMetadataDto: &FilesMetadataDto{TarFileSets: map[string][]string{"a.tar": {"a"}, "b.tar": {"b"}}},
			}
			for name, size := range map[string]int{"a.tar": 1024, "b.tar": 2048, "pg_control.tar": 128, "backup_label.tar": 128} {
				require.NoError(t, backup.GetTarPartitionFolder().PutObject(ctx, name, strings.NewReader(strings.Repeat("x", size))))
			}
			frames := make(archiveProgressFrames, 64)
			tracker := progress.Start(ctx, frames, "backup-fetch", 160)
			defer tracker.Stop()
			ctx = progress.WithTracker(ctx, tracker)
			backup.setRestoreProgress(ctx)
			files := map[string]bool{"a": true}
			if tc.noFiles {
				files = map[string]bool{}
			}
			concurrent, sequential, err := (FilesToExtractProviderImpl{}).Get(ctx, backup, files, tc.skip)
			require.NoError(t, err)
			assert.Equal(t, 1, lists, "reuse the one existing tar listing")
			if tc.total == "" {
				frame := awaitArchiveProgress(t, frames, "restoring full")
				assert.NotContains(t, frame, "ETA")
				return
			}
			awaitArchiveProgress(t, frames, "0 B/"+tc.total+" | ETA --")
			readers := concurrent
			if !tc.legacy {
				readers = append(readers, sequential...)
			}
			for _, maker := range readers {
				r, err := maker.Reader(ctx)
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, r)
				require.NoError(t, err)
				require.NoError(t, r.Close())
			}
			awaitArchiveProgress(t, frames, tc.total+"/"+tc.total+" | ETA 0s")
			// Reopening an archive must disable the estimate before repeated bytes count.
			r, err := concurrent[0].Reader(ctx)
			require.NoError(t, err)
			require.NoError(t, r.Close())
			frame := awaitArchiveProgress(t, frames, "backup-fetch [")
			assert.NotContains(t, frame, "ETA")
			assert.Equal(t, 1, lists)
		})
	}
}

type progressListingFolder struct {
	storage.Folder
	lists *int
	sizes map[string]int64
}

func (f *progressListingFolder) GetSubFolder(path string) storage.Folder {
	return &progressListingFolder{Folder: f.Folder.GetSubFolder(path), lists: f.lists, sizes: f.sizes}
}

func (f *progressListingFolder) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	*f.lists++
	objects, folders, err := f.Folder.ListFolder(ctx)
	for i, object := range objects {
		if size, ok := f.sizes[object.GetName()]; ok {
			objects[i] = storage.NewLocalObject(object.GetName(), object.GetLastModified(), size)
		}
	}
	return objects, folders, err
}

func (f *progressListingFolder) StatObject(context.Context, string) (storage.Object, error) {
	panic("download progress must not make extra HEAD requests")
}

type archiveProgressFrames chan string

func (f archiveProgressFrames) Write(b []byte) (int, error) {
	select {
	case f <- string(b):
	default:
	}
	return len(b), nil
}

func awaitArchiveProgress(t *testing.T, frames archiveProgressFrames, want string) string {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case frame := <-frames:
			if strings.Contains(frame, want) {
				return frame
			}
		case <-timer.C:
			t.Fatalf("progress did not report %q", want)
			return ""
		}
	}
}
