package oplog

import (
	"context"
	"fmt"
	"sync"

	"github.com/wal-g/wal-g/internal"

	"bytes"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/storage"
	"github.com/wal-g/wal-g/utility"
)

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(context.Context, chan Record, *sync.WaitGroup) (chan error, error)
}

// DBApplier implements Applier interface for mongodb.
type DBApplier struct {
	uri string
	mc  *internal.MongoClient
}

// NewDBApplier builds DBApplier with given args.
func NewDBApplier(uri string) *DBApplier {
	return &DBApplier{uri: uri}
}

// Apply runs working cycle that applies oplog records.
func (dba *DBApplier) Apply(ctx context.Context, ch chan Record, wg *sync.WaitGroup) (chan error, error) {
	mc, err := internal.NewMongoClient(ctx, dba.uri)
	if err != nil {
		return nil, err
	}
	if _, err := mc.GetOplogCollection(ctx); err != nil {
		return nil, err
	}

	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer mc.Close(ctx)
		defer close(errc)

		for op := range ch {
			tracelog.DebugLogger.Printf("Applyer receieved op %s (%s on %s)", op.TS, op.OP, op.NS)
			if err := mc.ApplyOp(ctx, op.Data); err != nil {
				errc <- fmt.Errorf("apply op (%s %s on %s) failed with: %w", op.TS, op.OP, op.NS, err)
				return
			}
		}
	}()

	return errc, nil
}

type StorageApplier struct {
	uploader *storage.Uploader
	size     int
	timeout  time.Duration
}

func NewStorageApplier(uploader *storage.Uploader, archiveAfterSize int, archiveTimeout time.Duration) *StorageApplier {
	return &StorageApplier{uploader, archiveAfterSize, archiveTimeout}
}

func (sa *StorageApplier) Apply(ctx context.Context, oplogc chan Record, wg *sync.WaitGroup) (chan error, error) {
	archiveTimer := time.NewTimer(sa.timeout)
	var lastKnownTS, batchStartTs Timestamp
	isFirstBatch := true

	errc := make(chan error)
	wg.Add(1)
	go func() {
		var buf bytes.Buffer // TODO: switch to tmp files

		defer wg.Done()
		defer close(errc)
		defer archiveTimer.Stop()
		for {
			select {
			case op, ok := <-oplogc:
				if !ok {
					return
				}
				if isFirstBatch {
					batchStartTs = op.TS
					isFirstBatch = false
				}

				lastKnownTS = op.TS
				buf.Write(op.Data)
				if buf.Len() < sa.size {
					continue
				}
				tracelog.DebugLogger.Println("Initializing archive upload due to archive size")

			case <-archiveTimer.C:
				if buf.Len() == 0 {
					utility.ResetTimer(archiveTimer, sa.timeout)
					continue
				}
				tracelog.DebugLogger.Println("Initializing archive upload due to timeout expired")
			}
			utility.ResetTimer(archiveTimer, sa.timeout)

			arch, err := NewArchive(batchStartTs, lastKnownTS, sa.uploader.Compressor.FileExtension())
			if err != nil {
				errc <- fmt.Errorf("can not build archive: %w", err)
				return
			}
			if err := sa.uploader.UploadStreamTo(&buf, arch.Filename()); err != nil {
				errc <- fmt.Errorf("can not upload archive: %w", err)
				return
			}

			buf.Reset()
			batchStartTs = lastKnownTS
		}
	}()

	return errc, nil
}
