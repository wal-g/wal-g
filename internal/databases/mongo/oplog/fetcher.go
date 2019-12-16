package oplog

import (
	"context"
	"fmt"
	"sync"

	"github.com/wal-g/wal-g/internal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Fetcher interface {
	GetOplogFrom(context.Context, Timestamp, *sync.WaitGroup) (chan Record, error)
}

type DBFetcher struct {
	uri string
	mc  *internal.MongoClient
}

func NewDBFetcher(uri string) *DBFetcher {
	return &DBFetcher{uri: uri}
}

// GetOplogFrom returns channel of oplog records, channel is filled in background.
// TODO: unit tests
// TODO: handle disconnects && stepdown
// TODO: use sessions
// TODO: use context.WithTimeout
func (db *DBFetcher) GetOplogFrom(ctx context.Context, fromTS Timestamp, wg *sync.WaitGroup) (chan Record, error) {
	mc, err := internal.NewMongoClient(ctx, db.uri)
	if err != nil {
		return nil, err
	}

	coll, err := mc.GetOplogCollection(ctx)
	if err != nil {
		return nil, err
	}

	ch := make(chan Record)
	wg.Add(1)
	go func(ctx context.Context, mc *internal.MongoClient, ch chan Record, wg *sync.WaitGroup) {
		defer wg.Done()
		defer close(ch)
		defer mc.Close(ctx)

		bsonTS := BsonTimestampFromOplogTS(fromTS)
		filter := bson.M{"ts": bson.M{"$gte": bsonTS}}

		cur, err := coll.Find(ctx, filter, options.Find().SetCursorType(options.TailableAwait))
		if err == nil && cur.ID() == 0 {
			err = fmt.Errorf("dead cursor from oplog find")
		}
		if err != nil {
			ch <- Record{Err: fmt.Errorf("oplog lookup failed: %w", err)}
			return
		}

		defer func() { _ = cur.Close(ctx) }()
		for cur.Next(ctx) {
			// TODO: benchmark decode vs. bson.Reader vs. bson.Raw.LookupErr
			opMeta := Meta{}
			if err := cur.Decode(&opMeta); err != nil {
				ch <- Record{Err: fmt.Errorf("oplog record decoding failed: %w", err)}
				return
			}
			ch <- Record{
				TS:   TimestampFromBson(opMeta.TS),
				OP:   opMeta.Op,
				NS:   opMeta.NS,
				Data: cur.Current,
			}
		}

		if err := cur.Err(); err != nil {
			if err == ctx.Err() {
				return
			}
			ch <- Record{Err: fmt.Errorf("oplog cursor error: %w", err)}
			return
		}
		ch <- Record{Err: fmt.Errorf("oplog cursor exhausted")}

	}(ctx, mc, ch, wg)

	return ch, nil
}
