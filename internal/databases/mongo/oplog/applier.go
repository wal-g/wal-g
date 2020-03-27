package oplog

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/txn"
	"go.mongodb.org/mongo-driver/bson"
)

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(ctx context.Context, opr models.Oplog) error
	Close(ctx context.Context) error
}

var _ Applier = &DBApplier{}

// DBApplier implements Applier interface for mongodb.
type DBApplier struct {
	db        client.MongoDriver
	txnBuffer *txn.Buffer
}

// NewDBApplier builds DBApplier with given args.
func NewDBApplier(m client.MongoDriver) *DBApplier {
	return &DBApplier{db: m, txnBuffer: txn.NewBuffer()}
}

func (dba *DBApplier) Apply(ctx context.Context, opr models.Oplog) error {
	op := db.Oplog{}
	if err := bson.Unmarshal(opr.Data, &op); err != nil {
		return fmt.Errorf("can not unmarshall oplog entry: %w", err)
	}

	meta, err := txn.NewMeta(op)
	if err != nil {
		return fmt.Errorf("can not extract op metadata: %w", err)
	}

	if meta.IsTxn() {
		err = dba.handleTxnOp(ctx, meta, op)
	} else {
		err = dba.handleNonTxnOp(ctx, op)
	}

	if err != nil {
		return fmt.Errorf("can not handle op: %w", err)
	}

	return nil
}

func (dba *DBApplier) Close(ctx context.Context) error {
	if err := dba.db.Close(ctx); err != nil {
		return err
	}
	if err := dba.txnBuffer.Stop(); err != nil {
		return err
	}
	return nil
}

// handleNonTxnOp tries to apply given oplog record.
// TODO: support UI filtering due to partial restore support
func (dba *DBApplier) handleNonTxnOp(ctx context.Context, op db.Oplog) error {
	return dba.db.ApplyOp(ctx, op)
}

// handleTxnOp handles oplog record with transaction attributes.
// TODO: unit test
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
