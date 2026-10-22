package postgres

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

// walPrefetchWindowSize is how many WAL segments ahead of the streaming
// position the prefetcher keeps downloaded on the local disk
const walPrefetchWindow = uint64(100)

// walSendPrefetcher keeps WAL segments downloaded ahead of the streaming
// position in a local prefetch directory, so that streaming reads from the
// local disk instead of waiting for the storage. The background loop keeps
// the window filled, redownloading whatever the stream has consumed.
type walSendPrefetcher struct {
	stream   *walSendStream
	dir      string
	reader   internal.StorageFolderReader
	timeline uint32
	window   uint64
}

// startWalSendPrefetcher creates the prefetch directory and starts the
// background download loop. The directory is removed when the context
// is canceled, i.e. when the replication stream ends.
func startWalSendPrefetcher(ctx context.Context, st *walSendStream,
	timeline uint32) (*walSendPrefetcher, error) {
	dir, err := os.MkdirTemp("", "walg-wal-send-prefetch-")
	if err != nil {
		return nil, err
	}
	prefetcher := &walSendPrefetcher{
		stream:   st,
		dir:      dir,
		reader:   st.client.server.reader,
		timeline: timeline,
		window:   walPrefetchWindow,
	}
	go prefetcher.loop(ctx)
	return prefetcher, nil
}

func (p *walSendPrefetcher) loop(ctx context.Context) {
	defer func() {
		if err := os.RemoveAll(p.dir); err != nil {
			tracelog.ErrorLogger.Printf("wal-send: remove the prefetch directory %s: %v", p.dir, err)
		}
	}()

	p.fillWindow(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.stream.client.server.pollInterval):
			p.fillWindow(ctx)
		}
	}
}

// fillWindow downloads the missing segments of the prefetch window,
// from the streaming position onwards
func (p *walSendPrefetcher) fillWindow(ctx context.Context) {
	currentSeg := uint64(p.stream.pos()) / WalSegmentSize
	for segNo := currentSeg; segNo < currentSeg+p.window; segNo++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		segmentFileName := formatWALFileName(p.timeline, segNo)
		if _, err := os.Stat(filepath.Join(p.dir, segmentFileName)); err == nil {
			continue // already prefetched
		}

		err := p.downloadSegment(ctx, segmentFileName)
		if err != nil {
			// stop at the first missing segment: the WAL must be
			// prefetched in order, the rest will arrive with the next
			// window refresh
			return
		}
	}
}

// downloadSegment downloads, decompresses and decrypts the segment from the
// storage into the prefetch directory, atomically (via a temp file + rename)
func (p *walSendPrefetcher) downloadSegment(ctx context.Context, segmentFileName string) error {
	segment, err := internal.DownloadAndDecompressStorageFile(ctx, p.reader, segmentFileName)
	if err != nil {
		return err // a missing or unreadable segment stops the window fill
	}
	defer utilityLoggedClose(segment, segmentFileName)

	tmpPath := filepath.Join(p.dir, segmentFileName+".tmp")
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	_, err = ioCopy(file, segment)
	utilityLoggedClose(file, tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	// only complete files are visible to the stream
	return os.Rename(tmpPath, filepath.Join(p.dir, segmentFileName))
}

// fetch returns the prefetched segment file, if it is already downloaded
func (p *walSendPrefetcher) fetch(segmentFileName string) (*os.File, bool) {
	file, err := os.Open(filepath.Join(p.dir, segmentFileName))
	if err != nil {
		return nil, false
	}
	return file, true
}

// remove drops the segment file the stream has finished streaming,
// keeping the prefetch directory bounded
func (p *walSendPrefetcher) remove(segmentFileName string) {
	_ = os.Remove(filepath.Join(p.dir, segmentFileName))
}

func utilityLoggedClose(closer io.Closer, what string) {
	utility.LoggedClose(closer, "wal-send: prefetch "+what)
}

func ioCopy(dst io.Writer, src io.Reader) (int64, error) {
	return io.Copy(dst, src)
}
