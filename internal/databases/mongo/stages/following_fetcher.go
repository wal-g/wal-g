package stages

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const oplogFollowListingLookbackSeconds = 300

type FollowStatus struct {
	Head       models.Timestamp
	Idle       bool
	ObservedAt time.Time
}

type OplogStorageGapError struct {
	After         models.Timestamp
	AvailableHead models.Timestamp
}

func (e *OplogStorageGapError) Error() string {
	return fmt.Sprintf("oplog archive chain after %s is broken while storage contains archives through %s",
		e.After, e.AvailableHead)
}

type followStopRequest struct {
	expectedHead models.Timestamp
	resultC      chan followStopResult
}

type followStopResult struct {
	stopped bool
	err     error
}

type FollowingStorageFetcher struct {
	downloader   archive.Downloader
	pollInterval time.Duration
	statusC      chan FollowStatus
	stopC        chan followStopRequest
}

func NewFollowingStorageFetcher(
	downloader archive.Downloader,
	pollInterval time.Duration,
) *FollowingStorageFetcher {
	return &FollowingStorageFetcher{
		downloader:   downloader,
		pollInterval: pollInterval,
		statusC:      make(chan FollowStatus, 1),
		stopC:        make(chan followStopRequest),
	}
}

func (f *FollowingStorageFetcher) Status() <-chan FollowStatus {
	return f.statusC
}

func (f *FollowingStorageFetcher) TryStop(ctx context.Context, expectedHead models.Timestamp) (bool, error) {
	request := followStopRequest{
		expectedHead: expectedHead,
		resultC:      make(chan followStopResult, 1),
	}
	select {
	case f.stopC <- request:
	case <-ctx.Done():
		return false, ctx.Err()
	}

	select {
	case result := <-request.resultC:
		return result.stopped, result.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (f *FollowingStorageFetcher) FetchFrom(
	ctx context.Context,
	from models.Timestamp,
	resumeAfter bool,
) (chan *models.Oplog, chan error, error) {
	oplogC := make(chan *models.Oplog)
	errC := make(chan error, 1)

	go func() {
		defer close(f.statusC)
		defer close(errC)
		defer close(oplogC)

		if err := f.run(ctx, from, resumeAfter, oplogC); err != nil {
			errC <- err
		}
	}()

	return oplogC, errC, nil
}

func (f *FollowingStorageFetcher) run(
	ctx context.Context,
	from models.Timestamp,
	resumeAfter bool,
	oplogC chan<- *models.Oplog,
) error {
	cursor := from
	for {
		path, head, err := f.discover(ctx, cursor, resumeAfter)
		if err != nil {
			return err
		}
		if len(path) > 0 {
			f.publishStatus(FollowStatus{Head: head, ObservedAt: time.Now()})
			for _, arch := range path {
				if err := f.streamArchive(ctx, arch, cursor, resumeAfter, oplogC); err != nil {
					return err
				}
				cursor = arch.End
				resumeAfter = true
			}
			continue
		}

		f.publishStatus(FollowStatus{Head: cursor, Idle: true, ObservedAt: time.Now()})
		timer := time.NewTimer(f.pollInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-timer.C:
			continue
		case request := <-f.stopC:
			stopTimer(timer)
			path, _, discoverErr := f.discover(ctx, cursor, true)
			if discoverErr != nil {
				request.resultC <- followStopResult{err: discoverErr}
				return discoverErr
			}
			if len(path) > 0 || !models.Equal(cursor, request.expectedHead) {
				request.resultC <- followStopResult{}
				continue
			}
			request.resultC <- followStopResult{stopped: true}
			return nil
		}
	}
}

func (f *FollowingStorageFetcher) discover(
	ctx context.Context,
	cursor models.Timestamp,
	resumeAfter bool,
) (archive.Sequence, models.Timestamp, error) {
	lowerSecond := uint32(0)
	if cursor.TS > oplogFollowListingLookbackSeconds {
		lowerSecond = cursor.TS - oplogFollowListingLookbackSeconds
	}
	lower := models.Timestamp{TS: lowerSecond}
	oplogStart := fmt.Sprintf("%s_%s", models.ArchiveTypeOplog, lower)
	oplogs, err := f.downloader.ListOplogArchivesSegment(ctx, &oplogStart, nil)
	if err != nil {
		return nil, cursor, err
	}

	gapStart := fmt.Sprintf("%s_%s", models.ArchiveTypeGap, lower)
	gapEnd := models.ArchiveTypeGap + "_~"
	gaps, err := f.downloader.ListOplogArchivesSegment(ctx, &gapStart, &gapEnd)
	if err != nil {
		return nil, cursor, err
	}

	archives := deduplicateArchives(append(oplogs, gaps...))
	path, err := archive.LongestOplogSequenceFrom(archives, cursor, resumeAfter)
	if err != nil {
		return nil, cursor, err
	}
	if len(path) > 0 {
		return path, path[len(path)-1].End, nil
	}

	availableHead := cursor
	for _, arch := range archives {
		if models.LessTS(availableHead, arch.End) {
			availableHead = arch.End
		}
	}
	if models.LessTS(cursor, availableHead) {
		return nil, cursor, &OplogStorageGapError{After: cursor, AvailableHead: availableHead}
	}
	return nil, cursor, nil
}

func (f *FollowingStorageFetcher) streamArchive(
	ctx context.Context,
	arch models.Archive,
	from models.Timestamp,
	resumeAfter bool,
	oplogC chan<- *models.Oplog,
) error {
	tracelog.DebugLogger.Printf("Fetching archive %s", arch.Filename())
	reader, writer := io.Pipe()
	downloadErrC := make(chan error, 1)
	go func() {
		err := f.downloader.DownloadOplogArchive(ctx, arch, writer)
		if err != nil {
			_ = writer.CloseWithError(fmt.Errorf("failed to download archive %s: %w", arch.Filename(), err))
		} else {
			_ = writer.Close()
		}
		downloadErrC <- err
	}()

	bufferedReader := bufio.NewReaderSize(reader, 4*1024*1024)
	for {
		raw, err := bson.ReadDocument(bufferedReader)
		if err != nil {
			_ = reader.Close()
			downloadErr := <-downloadErrC
			if err == io.EOF && downloadErr == nil {
				return nil
			}
			if downloadErr != nil {
				return fmt.Errorf("failed to download archive %s: %w", arch.Filename(), downloadErr)
			}
			return fmt.Errorf("error during read bson from archive %s: %w", arch.Filename(), err)
		}

		op, err := models.OplogFromRaw(raw)
		if err != nil {
			_ = reader.CloseWithError(err)
			<-downloadErrC
			return fmt.Errorf("oplog record decoding failed: %w", err)
		}
		if models.LessTS(op.TS, from) || resumeAfter && models.Equal(op.TS, from) {
			models.PutOplogEntry(op)
			continue
		}

		select {
		case oplogC <- op:
		case <-ctx.Done():
			_ = reader.CloseWithError(ctx.Err())
			<-downloadErrC
			return ctx.Err()
		}
	}
}

func (f *FollowingStorageFetcher) publishStatus(status FollowStatus) {
	select {
	case f.statusC <- status:
		return
	default:
	}
	select {
	case <-f.statusC:
	default:
	}
	select {
	case f.statusC <- status:
	default:
	}
}

func deduplicateArchives(archives []models.Archive) []models.Archive {
	seen := make(map[string]struct{}, len(archives))
	result := make([]models.Archive, 0, len(archives))
	for _, arch := range archives {
		name := arch.Filename()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, arch)
	}
	return result
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
