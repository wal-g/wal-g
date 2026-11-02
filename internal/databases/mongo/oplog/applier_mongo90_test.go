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
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var timeseriesUUID = bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte("0123456789abcdef")}

func commandEntry(t *testing.T, ts uint32, ns string, object bson.D, ui *bson.Binary) models.Oplog {
	t.Helper()
	op := db.Oplog{
		Timestamp: bson.Timestamp{T: 100, I: ts},
		Operation: "c",
		Namespace: ns,
		Object:    object,
		UI:        ui,
	}
	data, err := bson.Marshal(op)
	require.NoError(t, err)
	return models.Oplog{TS: models.Timestamp{TS: 100, Inc: ts}, Data: data}
}

func dropIdentCommand() bson.D {
	return bson.D{{Key: "dropIdent", Value: "collection-7--4104909142373009110"}}
}

func timeseriesConversionEntry(t *testing.T, isUpgrade bool, ui *bson.Binary) models.Oplog {
	t.Helper()
	return commandEntry(t, 1, "test.$cmd", bson.D{
		{Key: "upgradeDowngradeViewlessTimeseries", Value: "weather"},
		{Key: "isUpgrade", Value: isUpgrade},
	}, ui)
}

func TestDBApplierSkipsDropIdent(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	require.NoError(t, applier.Apply(t.Context(), commandEntry(t, 1, "admin.$cmd", dropIdentCommand(), nil)))
	opTime, ok := applier.LastAppliedOpTime()
	require.True(t, ok)
	require.Equal(t, models.Timestamp{TS: 100, Inc: 1}, opTime.TS)
}

func TestDBApplierSkipsNestedDropIdent(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOp", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			require.Equal(t, []string{"test.items"}, nestedNamespaces(t, args.Get(1).(*db.Oplog)))
		}).Return(nil).Once()
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	entry := applyOpsEntry(t, 1, bson.D{{Key: "applyOps", Value: bson.A{
		bson.D{{Key: "op", Value: "c"}, {Key: "ns", Value: "admin.$cmd"}, {Key: "o", Value: dropIdentCommand()}},
		nestedInsert("test.items", 1),
	}}})

	require.NoError(t, applier.Apply(t.Context(), entry))
}

func TestDBApplierKeepsUUIDForTimeseriesConversion(t *testing.T) {
	for _, isUpgrade := range []bool{true, false} {
		mongoDB := clientmocks.NewMongoDriver(t)
		mongoDB.On("ApplyOp", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				op := args.Get(1).(*db.Oplog)
				require.Equal(t, "upgradeDowngradeViewlessTimeseries", op.Object[0].Key)
				require.Equal(t, &timeseriesUUID, op.UI)
			}).Return(nil).Once()
		applier := NewDBApplier(mongoDB, DBApplierArgs{PreserveUUID: false})

		require.NoError(t, applier.Apply(t.Context(), timeseriesConversionEntry(t, isUpgrade, &timeseriesUUID)))
		require.NoError(t, applier.Close(context.Background()))
	}
}

func TestDBApplierRejectsTimeseriesConversionWithoutUUID(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	err := applier.Apply(t.Context(), timeseriesConversionEntry(t, true, nil))
	require.ErrorContains(t, err, "upgradeDowngradeViewlessTimeseries")
	require.ErrorContains(t, err, "UUID")
}

func TestDBApplierReturnsTimeseriesConversionError(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	mongoDB.On("ApplyOp", mock.Anything, mock.Anything).
		Return(mongo.CommandError{Code: 11483005, Message: "The buckets collection to upgrade does not have the expected UUID"}).
		Once()
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	err := applier.Apply(t.Context(), timeseriesConversionEntry(t, true, &timeseriesUUID))
	require.ErrorContains(t, err, "setFeatureCompatibilityVersion")
	require.ErrorContains(t, err, "expected UUID")
}

func TestDBApplierRejectsInitReplicatedFastCount(t *testing.T) {
	mongoDB := clientmocks.NewMongoDriver(t)
	applier := NewDBApplier(mongoDB, DBApplierArgs{})
	defer applier.Close(context.Background())

	entry := commandEntry(t, 1, "admin.$cmd", bson.D{{Key: "initReplicatedFastCount", Value: int32(1)}}, nil)

	require.ErrorContains(t, applier.Apply(t.Context(), entry), "initReplicatedFastCount")
}
