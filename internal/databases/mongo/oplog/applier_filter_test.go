package oplog

import (
	"context"
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientmocks "github.com/wal-g/wal-g/internal/databases/mongo/client/mocks"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func nestedInsert(ns string, id int32) bson.D {
	return bson.D{
		{Key: "op", Value: "i"},
		{Key: "ns", Value: ns},
		{Key: "o", Value: bson.D{{Key: "_id", Value: id}}},
	}
}

func txnEntry(t *testing.T, ts uint32, object bson.D) models.Oplog {
	t.Helper()
	lsid, err := bson.Marshal(bson.D{{Key: "id", Value: "session"}})
	require.NoError(t, err)
	txnNumber := int64(1)
	op := db.Oplog{
		Timestamp: bson.Timestamp{T: 100, I: ts},
		Operation: "c",
		Namespace: "admin.$cmd",
		Object:    object,
		LSID:      lsid,
		TxnNumber: &txnNumber,
	}
	data, err := bson.Marshal(op)
	require.NoError(t, err)
	return models.Oplog{TS: models.Timestamp{TS: 100, Inc: ts}, Data: data}
}

func applyOpsEntry(t *testing.T, ts uint32, object bson.D) models.Oplog {
	t.Helper()
	op := db.Oplog{
		Timestamp: bson.Timestamp{T: 100, I: ts},
		Operation: "c",
		Namespace: "admin.$cmd",
		Object:    object,
	}
	data, err := bson.Marshal(op)
	require.NoError(t, err)
	return models.Oplog{TS: models.Timestamp{TS: 100, Inc: ts}, Data: data}
}

func nestedNamespaces(t *testing.T, op *db.Oplog) []string {
	t.Helper()
	ops, err := unwrapNestedApplyOps(&op.Object)
	require.NoError(t, err)
	namespaces := make([]string, 0, len(ops))
	for i := range ops {
		namespaces = append(namespaces, ops[i].Namespace)
	}
	return namespaces
}

func TestDBApplierAbortedPreparedTransactionIsNotPending(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	prepare := txnEntry(t, 1, bson.D{
		{Key: "applyOps", Value: bson.A{nestedInsert("test.items", 1)}},
		{Key: "prepare", Value: true},
	})
	abort := txnEntry(t, 2, bson.D{{Key: "abortTransaction", Value: int32(1)}})

	require.NoError(t, applier.Apply(t.Context(), prepare))
	require.True(t, applier.HasPendingTransactions())
	require.NoError(t, applier.Apply(t.Context(), abort))
	require.False(t, applier.HasPendingTransactions())
}

func TestDBApplierAbortWithoutPreparedTransactionInWindow(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	abort := txnEntry(t, 1, bson.D{{Key: "abortTransaction", Value: int32(1)}})

	require.NoError(t, applier.Apply(t.Context(), abort))
	require.False(t, applier.HasPendingTransactions())
}

func TestDBApplierFiltersNestedApplyOps(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOp", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			op := args.Get(1).(*db.Oplog)
			require.Equal(t, []string{"test.items"}, nestedNamespaces(t, op))
		}).Return(nil).Once()
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	entry := applyOpsEntry(t, 1, bson.D{{Key: "applyOps", Value: bson.A{
		nestedInsert("config.cache.collections", 1),
		nestedInsert("test.items", 2),
		nestedInsert("config.system.sessions", 3),
	}}})

	require.NoError(t, applier.Apply(t.Context(), entry))
}

func TestDBApplierSkipsApplyOpsWithOnlyExcludedNestedOps(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	entry := applyOpsEntry(t, 1, bson.D{{Key: "applyOps", Value: bson.A{
		nestedInsert("config.cache.collections", 1),
	}}})

	require.NoError(t, applier.Apply(t.Context(), entry))
}

func TestDBApplierFiltersTransactionOps(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOp", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			require.Equal(t, "test.items", args.Get(1).(*db.Oplog).Namespace)
		}).Return(nil).Once()
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	entry := txnEntry(t, 1, bson.D{{Key: "applyOps", Value: bson.A{
		nestedInsert("config.system.sessions", 1),
		nestedInsert("test.items", 2),
	}}})

	require.NoError(t, applier.Apply(t.Context(), entry))
	require.False(t, applier.HasPendingTransactions())
}

func TestFilterNestedApplyOpsKeepsCommandFields(t *testing.T) {
	op := db.Oplog{
		Operation: "c",
		Namespace: "admin.$cmd",
		Object: bson.D{
			{Key: "applyOps", Value: bson.A{
				nestedInsert("config.cache.chunks.test.items", 1),
				nestedInsert("test.items", 2),
			}},
			{Key: "allowAtomic", Value: false},
		},
	}

	allFiltered, err := filterNestedApplyOps(&op, false)
	require.NoError(t, err)
	require.False(t, allFiltered)
	require.Equal(t, []string{"test.items"}, nestedNamespaces(t, &op))
	require.Equal(t, "allowAtomic", op.Object[1].Key)
}

func TestFilterNestedApplyOpsRewritesDeeplyNestedOps(t *testing.T) {
	op := db.Oplog{
		Operation: "c",
		Namespace: "admin.$cmd",
		Object: bson.D{{Key: "applyOps", Value: bson.A{
			bson.D{
				{Key: "op", Value: "c"},
				{Key: "ns", Value: "admin.$cmd"},
				{Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{
					nestedInsert("config.cache.collections", 1),
					nestedInsert("test.items", 2),
				}}}},
			},
		}}},
	}

	allFiltered, err := filterNestedApplyOps(&op, false)
	require.NoError(t, err)
	require.False(t, allFiltered)

	outer, err := unwrapNestedApplyOps(&op.Object)
	require.NoError(t, err)
	require.Len(t, outer, 1)
	require.Equal(t, []string{"test.items"}, nestedNamespaces(t, &outer[0]))
}
