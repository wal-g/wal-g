package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/internal/databases/mongo/stats"
	"github.com/wal-g/wal-g/utility"
)

var (
	_ = []Applier{&GenericApplier{}, &StorageApplier{}}
)

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(context.Context, chan *models.Oplog) (chan error, error)
}

// DBApplier implements Applier interface for mongodb.
type GenericApplier struct {
	applier oplog.Applier
}

// NewDBApplier builds DBApplier with given args.
func NewGenericApplier(applier oplog.Applier) *GenericApplier {
	return &GenericApplier{applier}
}

// Apply runs working cycle that applies oplog records.
func (dba *GenericApplier) Apply(ctx context.Context, ch chan *models.Oplog) (chan error, error) {
	errc := make(chan error)
	go func() {
		defer close(errc)
		defer func() { _ = dba.applier.Close(ctx) }()

		for opr := range ch {
			// we still pass oplog records in generic appliers by value
			if err := dba.applier.Apply(ctx, *opr); err != nil {
				errc <- fmt.Errorf("can not handle op: %w", err)
				return
			}
		}
	}()

	return errc, nil
}

// StorageApplier implements Applier interface for storage.
type StorageApplier struct {
	uploader     archive.Uploader
	buf          Buffer
	size         int
	timeout      time.Duration
	statsUpdater stats.OplogUploadStatsUpdater
}

// NewStorageApplier builds StorageApplier.
// TODO: switch to functional options
func NewStorageApplier(uploader archive.Uploader,
	buf Buffer,
	archiveAfterSize int,
	archiveTimeout time.Duration,
	statsUpdater stats.OplogUploadStatsUpdater) *StorageApplier {
	return &StorageApplier{uploader, buf, archiveAfterSize, archiveTimeout, statsUpdater}
}

// Apply runs working cycle that sends oplog records to storage.
func (sa *StorageApplier) Apply(ctx context.Context, oplogc chan *models.Oplog) (chan error, error) {
	archiveTimer := time.NewTimer(sa.timeout)
	var lastKnownTS, batchStartTS models.Timestamp
	restartBatch := true
	batchDocs := 0
	batchSize := 0
	errc := make(chan error)
	go func() {
		// wait for upload succeed, close(errc) and will exit main process
		defer close(errc)
		defer archiveTimer.Stop()
		for oplogc != nil {
			select {
			case op, ok := <-oplogc:
				if !ok {
					// wait for oplogc close (ctx.Done() or error occurs in fetcher function)
					// then will break this select block, and dump the data in memory.
					oplogc = nil
					break
				}
				if restartBatch {
					batchStartTS = op.TS
					restartBatch = false
				}
				lastKnownTS = op.TS
				if _, err := sa.buf.Write(op.Data); err != nil {
					errc <- fmt.Errorf("can not write op to buffer: %w", err)
					return
				}
				batchDocs++
				models.PutOplogEntry(op)
				if sa.buf.Len() < sa.size {
					continue
				}
				tracelog.DebugLogger.Println("Initializing archive upload due to archive size")

			case <-archiveTimer.C:
				tracelog.DebugLogger.Println("Initializing archive upload due to timeout expired")
			}

			utility.ResetTimer(archiveTimer, sa.timeout)
			batchSize = sa.buf.Len()
			if batchSize == 0 {
				continue
			}

			bufReader, err := sa.buf.Reader()
			if err != nil {
				errc <- fmt.Errorf("can not get reader from buffer: %w", err)
				return
			}

			// TODO: move upload to the next stage, batch accumulation should not be blocked by upload
			// or switch to PushStreamToDestination (async api):
			// we don't know archive name beforehand, so upload stream and rename key (it leads to failures and require gc)
			// but consumes less memory
			if err := sa.uploader.UploadOplogArchive(bufReader, batchStartTS, lastKnownTS); err != nil {
				errc <- fmt.Errorf("can not upload oplog archive: %w", err)
				return
			}
			if sa.statsUpdater != nil {
				sa.statsUpdater.Update(batchDocs, batchSize, lastKnownTS)
			}
			batchDocs = 0
			if err := sa.buf.Reset(); err != nil {
				errc <- fmt.Errorf("can not reset buffer for reuse: %w", err)
				return
			}
			batchStartTS = lastKnownTS
		}
	}()

	return errc, nil
}
