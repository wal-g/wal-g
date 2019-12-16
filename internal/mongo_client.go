package internal

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
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
