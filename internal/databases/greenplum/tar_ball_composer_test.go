package greenplum

import (
	"archive/tar"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

func TestGpTarBallComposerAddFileMTime(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mtimeOffset  time.Duration
		futureOffset time.Duration
		queueDelay   time.Duration
		wantWait     time.Duration
		cancel       bool
		wantError    bool
	}{
		{name: "whole second", wantWait: time.Second},
		{name: "fractional second", mtimeOffset: 200 * time.Millisecond,
			queueDelay: 250 * time.Millisecond, wantWait: 550 * time.Millisecond},
		{name: "aged in queue", queueDelay: 1500 * time.Millisecond},
		{name: "next second boundary", queueDelay: time.Second},
		{name: "cancel while waiting", cancel: true},
		{name: "future within current second", futureOffset: 500 * time.Millisecond, wantWait: time.Second},
		{name: "future across second boundary", mtimeOffset: 750 * time.Millisecond,
			futureOffset: 500 * time.Millisecond, wantWait: 1250 * time.Millisecond},
		{name: "exactly one second ahead", futureOffset: time.Second, wantWait: 2 * time.Second},
		{name: "more than one second ahead", futureOffset: time.Second + time.Nanosecond, wantError: true},
		{name: "far in the future", futureOffset: time.Hour, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				time.Sleep(tc.mtimeOffset)
				mtime := time.Now().Add(tc.futureOffset)
				header := &tar.Header{Name: "/base/13/1663.1", Size: 100, Mode: 0o600, ModTime: mtime}
				cfi := internal.NewComposeFileInfo(header.Name, header.FileInfo(), true, false, header)
				location := walparser.NewBlockLocation(1663, 13, 1663, 1)
				meta := ao.NewRelFileMetadata("relation", ao.AppendOptimized, 100, 4)
				baseFiles := ao.BackupFiles{
					header.Name: {
						StoragePath: "previous_aoseg", MTime: mtime, EOF: 100, ModCount: 4,
						InitialUploadTS: mtime,
					},
				}
				files := &internal.RegularBundleFiles{}
				uploader := ao.NewStorageUploader(
					internal.NewRegularUploader(nil, memory.NewFolder("", memory.NewKVS())),
					baseFiles, nil, files, true, 24*time.Hour, "new")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				composer := &GpTarBallComposer{
					ctx: ctx, relStorageMap: ao.RelFileStorageMap{*location: meta}, aoStorageUploader: uploader,
				}

				time.Sleep(tc.queueDelay)
				start := time.Now()
				done := make(chan error, 1)
				go func() { done <- composer.addFile(cfi) }()
				if tc.wantWait > 0 || tc.cancel {
					synctest.Wait()
					_, processed := files.GetUnderlyingMap().Load(header.Name)
					require.False(t, processed, "AO processing must not start before the next second")
				}
				if tc.cancel {
					cancel()
					require.ErrorIs(t, <-done, context.Canceled)
					require.Empty(t, uploader.GetFiles().Files)
				} else if tc.wantError {
					err := <-done
					require.ErrorContains(t, err, "more than one second ahead of current time")
					require.ErrorContains(t, err, cfi.Path)
					require.ErrorContains(t, err, mtime.Format(time.RFC3339Nano))
					require.ErrorContains(t, err, start.Format(time.RFC3339Nano))
					require.Empty(t, uploader.GetFiles().Files)
				} else {
					require.NoError(t, <-done)
					got := uploader.GetFiles().Files[header.Name]
					require.NotNil(t, got)
					require.True(t, got.IsSkipped)
					require.True(t, got.MTime.Equal(mtime), "preserve the mtime captured before waiting")
				}
				require.Equal(t, tc.wantWait, time.Since(start))
			})
		})
	}
}
