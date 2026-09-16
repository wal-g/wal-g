package oplog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientmocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func batchTestEntry(t *testing.T, ts uint32, operation string, object bson.D) models.Oplog {
	t.Helper()
	op := db.Oplog{
		Timestamp: bson.Timestamp{T: 100, I: ts},
		Operation: operation,
		Namespace: "test.items",
		Object:    object,
	}
	if operation == "c" {
		op.Namespace = "test.$cmd"
	}
	data, err := bson.Marshal(op)
	require.NoError(t, err)
	return models.Oplog{TS: models.Timestamp{TS: 100, Inc: ts}, Data: data}
}

func TestDBApplierBatchesFiftyCRUDOperations(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOps", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			ops := args.Get(1).([]*db.Oplog)
			require.Len(t, ops, 50)
			for i, op := range ops {
				require.Equal(t, "i", op.Operation)
				require.Equal(t, int32(i), op.Object[0].Value)
			}
		}).Return(nil).Once()

	entries := make([]models.Oplog, 50)
	for i := range entries {
		entries[i] = batchTestEntry(t, uint32(i+1), "i", bson.D{{Key: "_id", Value: int32(i)}})
	}
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())
	require.NoError(t, applier.ApplyBatch(t.Context(), entries))
	opTime, ok := applier.LastAppliedOpTime()
	require.True(t, ok)
	require.Equal(t, models.Timestamp{TS: 100, Inc: 50}, opTime.TS)
}

func TestDBApplierAllowsLargerBatch(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOps", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			require.Len(t, args.Get(1).([]*db.Oplog), 100)
		}).Return(nil).Once()

	entries := make([]models.Oplog, 100)
	for i := range entries {
		entries[i] = batchTestEntry(t, uint32(i+1), "i", bson.D{{Key: "_id", Value: int32(i)}})
	}
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())
	require.NoError(t, applier.ApplyBatch(t.Context(), entries))
}

func TestDBApplierDoesNotAdvanceOpTimeOnBatchFailure(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOps", mock.Anything, mock.Anything).Return(errors.New("partial failure")).Once()
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())
	entries := []models.Oplog{
		batchTestEntry(t, 1, "i", bson.D{{Key: "_id", Value: 1}}),
		batchTestEntry(t, 2, "i", bson.D{{Key: "_id", Value: 2}}),
	}
	require.ErrorContains(t, applier.ApplyBatch(t.Context(), entries), "partial failure")
	_, ok := applier.LastAppliedOpTime()
	require.False(t, ok)
}

func TestDBApplierPreservesCommandOrder(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	var calls []string
	mongoDB.On("ApplyOps", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			calls = append(calls, fmt.Sprintf("batch:%d", len(args.Get(1).([]*db.Oplog))))
		}).Return(nil).Twice()
	mongoDB.On("ApplyOp", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			require.Equal(t, "c", args.Get(1).(*db.Oplog).Operation)
			calls = append(calls, "command")
		}).Return(nil).Once()

	entries := []models.Oplog{
		batchTestEntry(t, 1, "i", bson.D{{Key: "_id", Value: 1}}),
		batchTestEntry(t, 2, "i", bson.D{{Key: "_id", Value: 2}}),
		batchTestEntry(t, 3, "c", bson.D{{Key: "drop", Value: "old"}}),
		batchTestEntry(t, 4, "i", bson.D{{Key: "_id", Value: 3}}),
	}
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())
	require.NoError(t, applier.ApplyBatch(t.Context(), entries))
	require.Equal(t, []string{"batch:2", "command", "batch:1"}, calls)
}

func TestDBApplierKeepsPartialRestoreErrorsPerOperation(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOp", mock.Anything, mock.Anything).Return(nil).Twice()
	applier := NewDBApplier(mongoDB, DBApplierArgs{Partial: true})
	defer applier.Close(context.Background())
	entries := []models.Oplog{
		batchTestEntry(t, 1, "i", bson.D{{Key: "_id", Value: 1}}),
		batchTestEntry(t, 2, "i", bson.D{{Key: "_id", Value: 2}}),
	}
	require.NoError(t, applier.ApplyBatch(t.Context(), entries))
}

func TestDBApplierKeepsTransactionSeparateFromCRUD(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	var calls []string
	mongoDB.On("ApplyOps", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			require.Len(t, args.Get(1).([]*db.Oplog), 1)
			calls = append(calls, "batch")
		}).Return(nil).Twice()
	mongoDB.On("ApplyOp", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			op := args.Get(1).(*db.Oplog)
			require.Equal(t, "i", op.Operation)
			calls = append(calls, fmt.Sprintf("transaction:%v", op.Object[0].Value))
		}).Return(nil).Twice()

	lsid, err := bson.Marshal(bson.D{{Key: "id", Value: "session"}})
	require.NoError(t, err)
	txnNumber := int64(1)
	txnOp := db.Oplog{
		Timestamp: bson.Timestamp{T: 100, I: 2},
		Operation: "c",
		Namespace: "admin.$cmd",
		Object: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "test.items"},
				{Key: "o", Value: bson.D{{Key: "_id", Value: int32(99)}}}},
			bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "test.items"},
				{Key: "o", Value: bson.D{{Key: "_id", Value: int32(100)}}}},
		}}},
		LSID:      lsid,
		TxnNumber: &txnNumber,
	}
	txnData, err := bson.Marshal(txnOp)
	require.NoError(t, err)
	entries := []models.Oplog{
		batchTestEntry(t, 1, "i", bson.D{{Key: "_id", Value: 1}}),
		{TS: models.Timestamp{TS: 100, Inc: 2}, Data: txnData},
		batchTestEntry(t, 3, "i", bson.D{{Key: "_id", Value: 3}}),
	}
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())
	require.NoError(t, applier.ApplyBatch(t.Context(), entries))
	require.Equal(t, []string{"batch", "transaction:99", "transaction:100", "batch"}, calls)
	require.False(t, applier.HasPendingTransactions())
}
