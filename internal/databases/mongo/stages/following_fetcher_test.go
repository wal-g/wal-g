package stages

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

type followingDownloaderStub struct {
	archive.Downloader
	mu        sync.Mutex
	archives  []models.Archive
	raw       map[string][]byte
	downloads map[string]int
}

func newFollowingDownloaderStub() *followingDownloaderStub {
	return &followingDownloaderStub{
		raw:       make(map[string][]byte),
		downloads: make(map[string]int),
	}
}

func (d *followingDownloaderStub) add(arch models.Archive, raw []byte) {
	d.mu.Lock()
	d.archives = append(d.archives, arch)
	d.raw[arch.Filename()] = raw
	d.mu.Unlock()
}

func (d *followingDownloaderStub) ListOplogArchivesSegment(
	_ context.Context,
	startAfter *string,
	_ *string,
) ([]models.Archive, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	wantGaps := startAfter != nil && strings.HasPrefix(*startAfter, models.ArchiveTypeGap+"_")
	result := make([]models.Archive, 0, len(d.archives))
	for _, arch := range d.archives {
		if (arch.Type == models.ArchiveTypeGap) == wantGaps {
			result = append(result, arch)
		}
	}
	return result, nil
}

func (d *followingDownloaderStub) DownloadOplogArchive(
	_ context.Context,
	arch models.Archive,
	writer io.WriteCloser,
) error {
	d.mu.Lock()
	raw := append([]byte(nil), d.raw[arch.Filename()]...)
	d.downloads[arch.Filename()]++
	d.mu.Unlock()
	_, err := writer.Write(raw)
	return err
}

func (d *followingDownloaderStub) downloadCount(arch models.Archive) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.downloads[arch.Filename()]
}

func TestFollowingStorageFetcherPicksUpNewArchiveAndRechecksBeforeStopping(t *testing.T) {
	archives, raws := ArchRawMocks(ops[:3], ops[3:5])
	downloader := newFollowingDownloaderStub()
	downloader.add(archives[0], raws[0])
	fetcher := NewFollowingStorageFetcher(downloader, time.Hour)

	oplogC, errC, err := fetcher.FetchFrom(t.Context(), ops[0].TS, false)
	require.NoError(t, err)
	for _, expected := range ops[:3] {
		require.Equal(t, expected.TS, receiveFollowOp(t, oplogC).TS)
	}
	waitForFollowIdle(t, fetcher.Status(), archives[0].End)

	downloader.add(archives[1], raws[1])
	stopped, err := fetcher.TryStop(t.Context(), archives[0].End)
	require.NoError(t, err)
	require.False(t, stopped)

	for _, expected := range ops[3:5] {
		require.Equal(t, expected.TS, receiveFollowOp(t, oplogC).TS)
	}
	waitForFollowIdle(t, fetcher.Status(), archives[1].End)
	stopped, err = fetcher.TryStop(t.Context(), archives[1].End)
	require.NoError(t, err)
	require.True(t, stopped)

	_, ok := <-oplogC
	require.False(t, ok)
	require.NoError(t, <-errC)
	require.Equal(t, 1, downloader.downloadCount(archives[0]))
	require.Equal(t, 1, downloader.downloadCount(archives[1]))
}

func TestFollowingStorageFetcherReportsBrokenChain(t *testing.T) {
	downloader := newFollowingDownloaderStub()
	disconnected, err := models.NewArchive(
		models.Timestamp{TS: ops[2].TS.TS}, ops[4].TS, archiveExt, models.ArchiveTypeOplog)
	require.NoError(t, err)
	downloader.add(disconnected, ops[4].Data)
	fetcher := NewFollowingStorageFetcher(downloader, time.Millisecond)

	_, errC, err := fetcher.FetchFrom(t.Context(), ops[0].TS, true)
	require.NoError(t, err)
	var gapErr *OplogStorageGapError
	require.ErrorAs(t, <-errC, &gapErr)
	require.Equal(t, ops[0].TS, gapErr.After)
	require.Equal(t, ops[4].TS, gapErr.AvailableHead)
}

func receiveFollowOp(t *testing.T, oplogC <-chan *models.Oplog) *models.Oplog {
	t.Helper()
	select {
	case op, ok := <-oplogC:
		require.True(t, ok)
		return op
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for followed oplog operation")
		return nil
	}
}

func waitForFollowIdle(t *testing.T, statusC <-chan FollowStatus, head models.Timestamp) {
	t.Helper()
	for {
		select {
		case status, ok := <-statusC:
			require.True(t, ok)
			if status.Idle && models.Equal(status.Head, head) {
				return
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for follower to become idle at %s", head)
		}
	}
}
