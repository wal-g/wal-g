package oplog

import (
	"github.com/google/uuid"
	"github.com/mongodb/mongo-tools-common/db"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
)

func TestFilterUUIDs(t *testing.T) {
	tests := []struct {
		withUUIDs            db.Oplog
		withoutUUIDsExpected db.Oplog
	}{
		{
			withUUIDs: db.Oplog{
				Timestamp: primitive.Timestamp{T: 4242, I: 1},
				HistoryID: 0,
				Version:   2,
				Operation: "c",
				Namespace: "config.$cmd",
				Object: bson.D{
					{Key: "create", Value: "sampledQueriesDiff"},
					{Key: "idIndex", Value: bson.D{
						{Key: "v", Value: 2},
						{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
						{Key: "name", Value: "_id_"},
					}},
				},
				UI: newUUIDBytes(t),
			},
			withoutUUIDsExpected: db.Oplog{
				Timestamp: primitive.Timestamp{T: 4242, I: 1},
				HistoryID: 0,
				Version:   2,
				Operation: "c",
				Namespace: "config.$cmd",
				Object: bson.D{
					{Key: "create", Value: "sampledQueriesDiff"},
					{Key: "idIndex", Value: bson.D{
						{Key: "v", Value: 2},
						{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
						{Key: "name", Value: "_id_"},
					}},
				},
			},
		},
		{
			withUUIDs: db.Oplog{
				Timestamp: primitive.Timestamp{T: 4242, I: 1},
				HistoryID: 0,
				Version:   2,
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{Key: "applyOps", Value: bson.A{
						bson.D{
							{Key: "op", Value: "d"},
							{Key: "ns", Value: "test.coll"},
							{Key: "ui", Value: newUUIDBytes(t)},
							{Key: "o", Value: bson.D{{Key: "_id", Value: newObjectID(t, "6554e57e27da07dc9041f340")}}},
						},
						bson.D{
							{Key: "op", Value: "d"},
							{Key: "ns", Value: "test.coll"},
							{Key: "ui", Value: newUUIDBytes(t)},
							{Key: "o", Value: bson.D{{Key: "_id", Value: newObjectID(t, "6554e57e27da07dc9041f341")}}},
						},
					}},
				},
			},
			withoutUUIDsExpected: db.Oplog{
				Timestamp: primitive.Timestamp{T: 4242, I: 1},
				HistoryID: 0,
				Version:   2,
				Operation: "c",
				Namespace: "admin.$cmd",
				Object: bson.D{
					{Key: "applyOps", Value: bson.A{
						// We're getting some extra fields here (ts, h, v), but it's ok:
						// operation is valid, parent's (ts, h, v) have priority
						bson.D{
							{Key: "ts", Value: primitive.Timestamp{T: 0, I: 0}},
							{Key: "h", Value: int64(0)},
							{Key: "v", Value: int32(0)},
							{Key: "op", Value: "d"},
							{Key: "ns", Value: "test.coll"},
							{Key: "o", Value: bson.D{{Key: "_id", Value: newObjectID(t, "6554e57e27da07dc9041f340")}}},
						},
						bson.D{
							{Key: "ts", Value: primitive.Timestamp{T: 0, I: 0}},
							{Key: "h", Value: int64(0)},
							{Key: "v", Value: int32(0)},
							{Key: "op", Value: "d"},
							{Key: "ns", Value: "test.coll"},
							{Key: "o", Value: bson.D{{Key: "_id", Value: newObjectID(t, "6554e57e27da07dc9041f341")}}},
						},
					}},
				},
			},
		},
	}

	for _, tt := range tests {
		withoutUUIDsActual, err := filterUUIDs(tt.withUUIDs)
		assert.NoError(t, err)

		assert.Equal(t, tt.withoutUUIDsExpected, withoutUUIDsActual)
	}
}

func newUUIDBytes(t *testing.T) *primitive.Binary {
	uuidBytes, err := uuid.New().MarshalBinary()
	assert.NoError(t, err)

	return &primitive.Binary{
		Subtype: 4,
		Data:    uuidBytes,
	}
}

func newObjectID(t *testing.T, hex string) primitive.ObjectID {
	objectID, err := primitive.ObjectIDFromHex(hex)
	assert.NoError(t, err)

	return objectID
}
