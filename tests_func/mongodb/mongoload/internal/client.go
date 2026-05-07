package internal

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func NewMongoClient(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect( options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return client, client.Ping(ctx, nil)
}
