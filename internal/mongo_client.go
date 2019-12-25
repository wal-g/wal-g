package internal

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	OplogDatabaseName   = "local"
	OplogCollectionName = "oplog.rs"
)

type MongoClient struct {
	c *mongo.Client
}

func NewMongoClient(ctx context.Context, uri string) (*MongoClient, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &MongoClient{c: client}, nil
}

func (mc *MongoClient) Close(ctx context.Context) {
	_ = mc.c.Disconnect(ctx)
}

func (mc *MongoClient) GetOplogCollection(ctx context.Context) (*mongo.Collection, error) {
	db := mc.c.Database(OplogDatabaseName)
	colls, err := db.ListCollectionNames(ctx, bson.M{"name": OplogCollectionName})
	if err != nil {
		return nil, fmt.Errorf("can not list collections in 'local' database: %w", err)
	}
	if len(colls) != 1 {
		return nil, fmt.Errorf("collection '%s' was not found in database '%s'",
			OplogCollectionName, OplogDatabaseName)
	}

	return db.Collection(OplogCollectionName), nil
}

type CmdResponse struct {
	Ok     int    `bson:"ok"`
	ErrMsg string `bson:"errmsg, omitempty"`
}

func (cr CmdResponse) String() string {
	return fmt.Sprintf("ok=%d (%s)", cr.Ok, cr.ErrMsg)
}

type Oplog struct {
	Timestamp primitive.Timestamp `bson:"ts"`
	HistoryID int64               `bson:"h"`
	Version   int                 `bson:"v"`
	Operation string              `bson:"op"`
	Namespace string              `bson:"ns"`
	Object    bson.D              `bson:"o"`
}

func (mc *MongoClient) ApplyOp(ctx context.Context, rawOp []byte) error {
	op := Oplog{}
	if err := bson.Unmarshal(rawOp, &op); err != nil {
		return fmt.Errorf("can not unmarshall oplog entry: %w", err)
	}

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
