package client

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/mongodb/mongo-tools-common/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	oplogDatabaseName   = "local"
	oplogCollectionName = "oplog.rs"
)

// CmdResponse is used to unmarshal mongodb cmd responses
type CmdResponse struct {
	Ok     int    `bson:"ok"`
	ErrMsg string `bson:"errmsg, omitempty"`
}

// Optime ...
type OpTime struct {
	TS   primitive.Timestamp `bson:"ts" json:"ts"`
	Term int64               `bson:"t" json:"t"`
}

// IsMasterLastWrite ...
type IsMasterLastWrite struct {
	OpTime         OpTime `bson:"opTime"`
	MajorityOpTime OpTime `bson:"majorityOpTime"`
}

// IsMaster is used to unmarshal results of IsMaster command
type IsMaster struct {
	IsMaster  bool              `bson:"ismaster"`
	LastWrite IsMasterLastWrite `bson:"lastWrite"`
}

// MongoDriver defines methods to work with mongodb.
type MongoDriver interface {
	IsMaster(ctx context.Context) (models.IsMaster, error)
	LastWriteTS(ctx context.Context) (lastTS, lastMajTS models.Timestamp, err error)
	TailOplogFrom(ctx context.Context, from models.Timestamp) (OplogCursor, error)
	ApplyOp(ctx context.Context, op db.Oplog) error
	Close(ctx context.Context) error
}

// OplogCursor defines methods to work with mongodb cursor.
type OplogCursor interface {
	Close(ctx context.Context) error
	Data() []byte
	Decode(val interface{}) error
	Err() error
	Next(context.Context) bool
}

// MongoOplogCursor implements OplogCursor.
type MongoOplogCursor struct {
	*mongo.Cursor
}

// NewMongoOplogCursor builds MongoOplogCursor.
func NewMongoOplogCursor(c *mongo.Cursor) *MongoOplogCursor {
	return &MongoOplogCursor{c}
}

// Data returns current cursor document
func (m *MongoOplogCursor) Data() []byte {
	return m.Current
}

// MongoClient implements MongoDriver
type MongoClient struct {
	c *mongo.Client
}

// NewMongoClient builds MongoClient
func NewMongoClient(ctx context.Context, uri string) (*MongoClient, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &MongoClient{c: client}, client.Ping(ctx, nil)
}

func (mc *MongoClient) IsMaster(ctx context.Context) (models.IsMaster, error) {
	im := IsMaster{}
	err := mc.c.Database("test").RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&im)
	if err != nil {
		return models.IsMaster{}, fmt.Errorf("isMaster command failed: %w", err)
	}

	return models.IsMaster{
		IsMaster: im.IsMaster,
		LastWrite: models.IsMasterLastWrite{
			OpTime: models.OpTime{
				TS: models.TimestampFromBson(im.LastWrite.OpTime.TS),
			},
			MajorityOpTime: models.OpTime{
				TS: models.TimestampFromBson(im.LastWrite.MajorityOpTime.TS),
			},
		},
	}, nil
}

// LastWriteTS fetches timestamps with last write
// TODO: support non-replset setups
func (mc *MongoClient) LastWriteTS(ctx context.Context) (lastTS, lastMajTS models.Timestamp, err error) {
	im, err := mc.IsMaster(ctx)
	if err != nil {
		return models.Timestamp{}, models.Timestamp{}, err
	}
	return im.LastWrite.OpTime.TS, im.LastWrite.MajorityOpTime.TS, nil
}

// Close disconnects from mongodb
func (mc *MongoClient) Close(ctx context.Context) error {
	return mc.c.Disconnect(ctx)
}

// TailOplogFrom gives OplogCursor to tail oplog from
func (mc *MongoClient) TailOplogFrom(ctx context.Context, from models.Timestamp) (OplogCursor, error) {
	coll, err := mc.getOplogCollection(ctx)
	if err != nil {
		return nil, err
	}

	bsonTS := models.BsonTimestampFromOplogTS(from)
	filter := bson.M{"ts": bson.M{"$gte": bsonTS}}
	cur, err := coll.Find(ctx, filter, options.Find().SetCursorType(options.TailableAwait))

	if err == nil && cur.ID() == 0 {
		err = fmt.Errorf("dead cursor from oplog find")
	}
	if err != nil {
		return nil, fmt.Errorf("oplog lookup failed: %w", err)
	}

	return NewMongoOplogCursor(cur), nil
}

func (mc *MongoClient) getOplogCollection(ctx context.Context) (*mongo.Collection, error) {
	odb := mc.c.Database(oplogDatabaseName)
	colls, err := odb.ListCollectionNames(ctx, bson.M{"name": oplogCollectionName})
	if err != nil {
		return nil, fmt.Errorf("can not list collections in 'local' database: %w", err)
	}
	if len(colls) != 1 {
		return nil, fmt.Errorf("collection '%s' was not found in database '%s'",
			oplogCollectionName, oplogDatabaseName)
	}

	return odb.Collection(oplogCollectionName), nil
}

// ApplyOp calls applyOps and check response
func (mc *MongoClient) ApplyOp(ctx context.Context, op db.Oplog) error {
	apply := mc.c.Database("admin").RunCommand(ctx, bson.M{"applyOps": []interface{}{op}})
	if err := apply.Err(); err != nil {
		return fmt.Errorf("applyOps command failed: %w\nop:\n%+v", err, op)
	}
	resp := CmdResponse{}
	if err := apply.Decode(&resp); err != nil {
		return fmt.Errorf("can not unmarshall command execution response: %w", err)
	}
	if resp.Ok != 1 {
		return fmt.Errorf("command execution failed with: %s", resp.ErrMsg)
	}

	return nil
}
