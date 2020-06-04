package stages

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
)

var (
	_ = []Applier{&GenericApplier{}, &StorageApplier{}}
)

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(context.Context, chan *models.Oplog, *sync.WaitGroup) (chan error, error)
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
func (dba *GenericApplier) Apply(ctx context.Context, ch chan *models.Oplog, wg *sync.WaitGroup) (chan error, error) {
	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = dba.applier.Close(ctx) }()
		defer close(errc)

		for opr := range ch {
			tracelog.DebugLogger.Printf("Applyer receieved op %s (%s on %s)", opr.TS, opr.OP, opr.NS)

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
	uploader archive.Uploader
	buf      Buffer
	size     int
	timeout  time.Duration
}

// NewStorageApplier builds StorageApplier.
func NewStorageApplier(uploader archive.Uploader, buf Buffer, archiveAfterSize int, archiveTimeout time.Duration) *StorageApplier {
	return &StorageApplier{uploader, buf, archiveAfterSize, archiveTimeout}
}

// Apply runs working cycle that sends oplog records to storage.
func (sa *StorageApplier) Apply(ctx context.Context, oplogc chan *models.Oplog, wg *sync.WaitGroup) (chan error, error) {
	archiveTimer := time.NewTimer(sa.timeout)
	var lastKnownTS, batchStartTs models.Timestamp
	restartBatch := true

	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errc)
		defer archiveTimer.Stop()
		for oplogc != nil {
			select {
			case op, ok := <-oplogc:
				if !ok {
					oplogc = nil
					break
				}
				if restartBatch {
					batchStartTs = op.TS
					restartBatch = false
				}
				lastKnownTS = op.TS
				if _, err := sa.buf.Write(op.Data); err != nil {
					errc <- fmt.Errorf("can not write op to buffer: %w", err)
					return
				}
				models.PutOplogEntry(op)
				if sa.buf.Len() < sa.size {
					continue
				}
				tracelog.DebugLogger.Println("Initializing archive upload due to archive size")

			case <-archiveTimer.C:
				tracelog.DebugLogger.Println("Initializing archive upload due to timeout expired")
			}

			utility.ResetTimer(archiveTimer, sa.timeout)
			if sa.buf.Len() == 0 {
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
			if err := sa.uploader.UploadOplogArchive(bufReader, batchStartTs, lastKnownTS); err != nil {
				errc <- fmt.Errorf("can not upload oplog archive: %w", err)
				return
			}

			if err := sa.buf.Reset(); err != nil {
				errc <- fmt.Errorf("can not reset buffer for reuse: %w", err)
				return
			}
			batchStartTs = lastKnownTS

		}
	}()

	return errc, nil
}
