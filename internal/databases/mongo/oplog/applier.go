package oplog

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/txn"
	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/bson"
)

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(context.Context, chan models.Oplog, *sync.WaitGroup) (chan error, error)
}

// DBApplier implements Applier interface for mongodb.
type DBApplier struct {
	db        client.MongoDriver
	txnBuffer *txn.Buffer
}

// NewDBApplier builds DBApplier with given args.
func NewDBApplier(m client.MongoDriver) *DBApplier {
	return &DBApplier{db: m}
}

// Apply runs working cycle that applies oplog records.
// TODO: filter noop, sessions ...
func (dba *DBApplier) Apply(ctx context.Context, ch chan models.Oplog, wg *sync.WaitGroup) (chan error, error) {
	dba.txnBuffer = txn.NewBuffer()

	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = dba.db.Close(ctx) }()
		defer func() { _ = dba.txnBuffer.Stop() }()
		defer close(errc)

		for opr := range ch {
			tracelog.DebugLogger.Printf("Applyer receieved op %s (%s on %s)", opr.TS, opr.OP, opr.NS)

			op := db.Oplog{}
			if err := bson.Unmarshal(opr.Data, &op); err != nil {
				errc <- fmt.Errorf("can not unmarshall oplog entry: %w", err)
				return
			}

			meta, err := txn.NewMeta(op)
			if err != nil {
				errc <- fmt.Errorf("can not extract op metadata: %w", err)
				return
			}

			if meta.IsTxn() {
				err = dba.handleTxnOp(ctx, meta, op)
			} else {
				err = dba.handleNonTxnOp(ctx, op)
			}

			if err != nil {
				errc <- fmt.Errorf("can not handle op: %w", err)
				return
			}
		}
	}()

	return errc, nil
}

// handleNonTxnOp tries to apply given oplog record.
// TODO: support UI filtering due to partial restore support
func (dba *DBApplier) handleNonTxnOp(ctx context.Context, op db.Oplog) error {
	if err := dba.db.ApplyOp(ctx, op); err != nil {
		return fmt.Errorf("apply op (%v %s on %s) failed with: %w", op.Timestamp, op.Operation, op.Namespace, err)
	}
	return nil
}

// handleTxnOp handles oplog record with transaction attributes.
func (dba *DBApplier) handleTxnOp(ctx context.Context, meta txn.Meta, op db.Oplog) error {
	if meta.IsAbort() {
		if err := dba.txnBuffer.PurgeTxn(meta); err != nil {
			return fmt.Errorf("can not clean txn buffer after rollback cmd: %w", err)
		}
	}
	if err := dba.txnBuffer.AddOp(meta, op); err != nil {
		return fmt.Errorf("can not append command to txn buffer: %w", err)
	}

	if !meta.IsCommit() {
		return nil
	}

	if err := dba.applyTxn(ctx, meta); err != nil {
		return err
	}

	if err := dba.txnBuffer.PurgeTxn(meta); err != nil {
		return fmt.Errorf("txn buffer failed to purge: %w", err)
	}

	return nil
}

func (dba *DBApplier) applyTxn(ctx context.Context, meta txn.Meta) error {
	opc, errc := dba.txnBuffer.GetTxnStream(meta)
	for {
		select {
		case op, ok := <-opc:
			if !ok {
				return nil
			}
			if err := dba.handleNonTxnOp(ctx, op); err != nil {
				return err
			}
		case err, ok := <-errc:
			if ok {
				return err
			}
		case <-ctx.Done():
			// opc and errc channels will be closed in PurgeTxn or Stop calls
			return nil
		}
	}
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
func (sa *StorageApplier) Apply(ctx context.Context, oplogc chan models.Oplog, wg *sync.WaitGroup) (chan error, error) {
	archiveTimer := time.NewTimer(sa.timeout)
	var lastKnownTS, batchStartTs models.Timestamp
	isFirstBatch := true
	//var skipNs = map[string]struct{}{
	//	"config.system.sessions":   {},
	//	"config.cache.collections": {},
	//	"config.mongos":            {},
	//	"config.lockpings":         {},
	//	"admin.system.version":     {},
	//	"admin.system.users":       {},
	//}

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
				//if _, ok := skipNs[op.NS]; ok {
				//	continue
				//}
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

			arch, err := models.NewArchive(batchStartTs, lastKnownTS, sa.uploader.FileExtension())
			if err != nil {
				errc <- fmt.Errorf("can not build archive: %w", err)
				return
			}

			if err := sa.uploader.UploadOplogArchive(&buf, arch); err != nil {
				errc <- fmt.Errorf("can not upload archive: %w", err)
				return
			}

			buf.Reset()
			batchStartTs = lastKnownTS
		}
	}()

	return errc, nil
}
