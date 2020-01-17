package client

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/mongodb/mongo-tools-common/db"
	"go.mongodb.org/mongo-driver/bson"
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

// MongoDriver defines methods to work with mongodb.
type MongoDriver interface {
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
		return fmt.Errorf("applyOps command failed: %w", err)
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
