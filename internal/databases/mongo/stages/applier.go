package stages

import (
	"bytes"
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

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(context.Context, chan models.Oplog, *sync.WaitGroup) (chan error, error)
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
func (dba *GenericApplier) Apply(ctx context.Context, ch chan models.Oplog, wg *sync.WaitGroup) (chan error, error) {
	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = dba.applier.Close(ctx) }()
		defer close(errc)

		for opr := range ch {
			tracelog.DebugLogger.Printf("Applyer receieved op %s (%s on %s)", opr.TS, opr.OP, opr.NS)

			if err := dba.applier.Apply(ctx, opr); err != nil {
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
	size     int
	timeout  time.Duration
}

// NewStorageApplier builds StorageApplier.
func NewStorageApplier(uploader archive.Uploader, archiveAfterSize int, archiveTimeout time.Duration) *StorageApplier {
	return &StorageApplier{uploader, archiveAfterSize, archiveTimeout}
}

// Apply runs working cycle that sends oplog records to storage.
// TODO: rename models.Oplog to something like models.Message
func (sa *StorageApplier) Apply(ctx context.Context, oplogc chan models.Oplog, wg *sync.WaitGroup) (chan error, error) {
	archiveTimer := time.NewTimer(sa.timeout)
	var lastKnownTS, batchStartTs models.Timestamp
	restartBatch := true

	errc := make(chan error)
	wg.Add(1)
	go func() {
		var buf bytes.Buffer

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
				buf.Write(op.Data)
				if buf.Len() < sa.size {
					continue
				}
				tracelog.DebugLogger.Println("Initializing archive upload due to archive size")

			case <-archiveTimer.C:
				tracelog.DebugLogger.Println("Initializing archive upload due to timeout expired")
			}

			utility.ResetTimer(archiveTimer, sa.timeout)
			if buf.Len() == 0 {
				continue
			}

			// TODO: switch to PushStreamToDestination (async api)
			// upload and rename (because we don't know last ts of uploading batch)
			if err := sa.uploader.UploadOplogArchive(&buf, batchStartTs, lastKnownTS); err != nil {
				errc <- fmt.Errorf("can not upload oplog archive: %w", err)
				return
			}

			buf.Reset()
			batchStartTs = lastKnownTS

		}
	}()

	return errc, nil
}
