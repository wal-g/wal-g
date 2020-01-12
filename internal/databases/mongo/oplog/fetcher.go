package oplog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// FromFetcher defines interface to fetch oplog records starting given timestamp.
type FromFetcher interface {
	OplogFrom(context.Context, Timestamp, *sync.WaitGroup) (chan Record, chan error, error)
}

// BetweenFetcher defines interface to fetch oplog records between given timestamps.
type BetweenFetcher interface {
	OplogBetween(context.Context, Timestamp, Timestamp, *sync.WaitGroup) (chan Record, chan error, error)
}

// DBFetcher implements FromFetcher interface for mongodb
type DBFetcher struct {
	uri string
	mc  *internal.MongoClient
}

// NewDBFetcher builds DBFetcher with given args.
func NewDBFetcher(uri string) *DBFetcher {
	return &DBFetcher{uri: uri}
}

// OplogFrom returns channel of oplog records, channel is filled in background.
// TODO: unit tests
// TODO: handle disconnects && stepdown
// TODO: use sessions
// TODO: use context.WithTimeout
func (dbf *DBFetcher) OplogFrom(ctx context.Context, fromTS Timestamp, wg *sync.WaitGroup) (oplogc chan Record, errc chan error, err error) {
	mc, err := internal.NewMongoClient(ctx, dbf.uri)
	if err != nil {
		return nil, nil, err
	}

	coll, err := mc.GetOplogCollection(ctx)
	if err != nil {
		return nil, nil, err
	}

	oplogc = make(chan Record)
	errc = make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errc)
		defer close(oplogc)
		defer mc.Close(ctx)

		bsonTS := BsonTimestampFromOplogTS(fromTS)
		filter := bson.M{"ts": bson.M{"$gte": bsonTS}}

		cur, err := coll.Find(ctx, filter, options.Find().SetCursorType(options.TailableAwait))
		if err == nil && cur.ID() == 0 {
			err = fmt.Errorf("dead cursor from oplog find")
		}
		if err != nil {
			errc <- fmt.Errorf("oplog lookup failed: %w", err)
			return
		}

		defer func() { _ = cur.Close(ctx) }()
		for cur.Next(ctx) {
			// TODO: benchmark decode vs. bson.Reader vs. bson.Raw.LookupErr
			opMeta := Meta{}
			if err := cur.Decode(&opMeta); err != nil {
				errc <- fmt.Errorf("oplog record decoding failed: %w", err)
				return
			}
			op := Record{
				TS:   TimestampFromBson(opMeta.TS),
				OP:   opMeta.Op,
				NS:   opMeta.NS,
				Data: cur.Current,
			}

			tracelog.DebugLogger.Printf("Fetcher receieved op %s (%s on %s)", op.TS, op.OP, op.NS)
			select {
			case oplogc <- op:
			case <-ctx.Done():
				return
			}
		}

		if err := cur.Err(); err != nil {
			if err == ctx.Err() {
				return
			}
			errc <- fmt.Errorf("oplog cursor error: %w", err)
			return
		}
		errc <- fmt.Errorf("oplog cursor exhausted")

	}()

	return oplogc, errc, nil
}

// CloserBuffer defines buffer which wraps bytes.Buffer and has dummy implementation of Closer interface.
type CloserBuffer struct {
	*bytes.Buffer
}

// NewCloserBuffer builds CloserBuffer instance
func NewCloserBuffer() *CloserBuffer {
	return &CloserBuffer{&bytes.Buffer{}}
}

// Close is dummy function that implements Closer interface.
func (cb *CloserBuffer) Close() error {
	return nil
}

// StorageFetcher implements BetweenFetcher interface for storage.
type StorageFetcher struct {
	folder storage.Folder
	path   ArchPath
}

// NewStorageFetcher builds StorageFetcher instance
func NewStorageFetcher(folder storage.Folder, path ArchPath) *StorageFetcher {
	return &StorageFetcher{folder: folder, path: path}
}

// OplogBetween returns channel of oplog records, channel is filled in background.
func (sf *StorageFetcher) OplogBetween(ctx context.Context, from, until Timestamp, wg *sync.WaitGroup) (chan Record, chan error, error) {
	data := make(chan Record)
	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(data)
		defer close(errc) // TODO: wait until error chan drained

		buf := NewCloserBuffer() // TODO: switch to streaming interface
		path := sf.path
		for _, arch := range path {
			tracelog.DebugLogger.Printf("Fetching archive %s", arch.Filename())
			err := internal.DownloadFile(sf.folder, arch.Filename(), arch.Extension(), buf)
			if err != nil {
				errc <- fmt.Errorf("failed to download archive %s: %w", arch.Filename(), err)
				return
			}

			for {
				// TODO: benchmark & compare with bson_stream
				raw, err := bson.NewFromIOReader(buf)
				if err != nil {
					if err == io.EOF {
						break
					}
					errc <- fmt.Errorf("error during read bson: %w", err)
				}

				opMeta := Meta{}
				if err := bson.Unmarshal(raw, &opMeta); err != nil {
					errc <- fmt.Errorf("oplog record decoding failed: %w", err)
					return
				}
				ts := TimestampFromBson(opMeta.TS)
				if Less(ts, from) {
					continue
				} else if Less(until, ts) || ts == until {
					tracelog.InfoLogger.Println("Oplog archives fetching is completed")
					return
				}

				select {
				case data <- Record{
					TS:   TimestampFromBson(opMeta.TS),
					OP:   opMeta.Op,
					NS:   opMeta.NS,
					Data: raw,
				}:
				case <-ctx.Done():
					tracelog.InfoLogger.Println("Oplog archives fetching is canceled")
					return
				}
			}
			buf.Reset()
		}
	}()

	return data, errc, nil
}
