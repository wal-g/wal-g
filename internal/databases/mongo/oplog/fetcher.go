package oplog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"

	"github.com/wal-g/tracelog"
	"go.mongodb.org/mongo-driver/bson"
)

// FromFetcher defines interface to fetch oplog records starting given timestamp.
type FromFetcher interface {
	OplogFrom(context.Context, models.Timestamp, *sync.WaitGroup) (chan models.Oplog, chan error, error)
}

// BetweenFetcher defines interface to fetch oplog records between given timestamps.
type BetweenFetcher interface {
	OplogBetween(context.Context, models.Timestamp, models.Timestamp, *sync.WaitGroup) (chan models.Oplog, chan error, error)
}

// DBFetcher implements FromFetcher interface for mongodb
type DBFetcher struct {
	db client.MongoDriver
}

// NewDBFetcher builds DBFetcher with given args.
func NewDBFetcher(m client.MongoDriver) *DBFetcher {
	return &DBFetcher{m}
}

// OplogFrom returns channel of oplog records, channel is filled in background.
// TODO: handle disconnects && stepdown
// TODO: use sessions
// TODO: use context.WithTimeout
func (dbf *DBFetcher) OplogFrom(ctx context.Context, from models.Timestamp, wg *sync.WaitGroup) (oplogc chan models.Oplog, errc chan error, err error) {
	cur, err := dbf.db.TailOplogFrom(ctx, from)
	if err != nil {
		return nil, nil, err
	}

	oplogc = make(chan models.Oplog)
	errc = make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errc)
		defer close(oplogc)
		defer func() { _ = cur.Close(ctx) }()

		fromFound := false

		for cur.Next(ctx) {
			// TODO: benchmark decode vs. bson.Reader vs. bson.Raw.LookupErr
			op, err := models.OplogFromRaw(cur.Data())
			if err != nil {
				errc <- fmt.Errorf("oplog record decoding failed: %w", err)
				return
			}

			if !fromFound {
				if op.TS != from { // from ts is not reached, continue
					errc <- fmt.Errorf("'from' timestamp '%s' was not found", from)
					return
				}
				fromFound = true
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
	downloader archive.Downloader
	path       archive.Sequence
}

// NewStorageFetcher builds StorageFetcher instance
func NewStorageFetcher(downloader archive.Downloader, path archive.Sequence) *StorageFetcher {
	return &StorageFetcher{downloader: downloader, path: path}
}

// OplogBetween returns channel of oplog records, channel is filled in background.
func (sf *StorageFetcher) OplogBetween(ctx context.Context, from, until models.Timestamp, wg *sync.WaitGroup) (chan models.Oplog, chan error, error) {
	if models.LessTS(until, from) {
		return nil, nil, fmt.Errorf("fromTS '%s' must be less than untilTS '%s'", from, until)
	}

	data := make(chan models.Oplog)
	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(data)
		defer close(errc)

		buf := NewCloserBuffer() // TODO: switch to streaming interface
		path := sf.path
		firstFound := false

		for _, arch := range path {
			tracelog.DebugLogger.Printf("Fetching archive %s", arch.Filename())

			err := sf.downloader.DownloadOplogArchive(arch, buf)
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

				op, err := models.OplogFromRaw(raw)
				if err != nil {
					errc <- fmt.Errorf("oplog record decoding failed: %w", err)
					return
				}

				if !firstFound {
					if op.TS != from { // from ts is not reached, continue
						continue
					}
					firstFound = true
				}

				// TODO: do we need also check every op "op.TS > from"
				if models.LessTS(until, op.TS) || op.TS == until {
					tracelog.InfoLogger.Println("Oplog archives fetching is completed")
					return
				}

				tracelog.DebugLogger.Printf("Fetcher receieved op %s (%s on %s)", op.TS, op.OP, op.NS)
				select {
				case data <- op:
				case <-ctx.Done():
					tracelog.InfoLogger.Println("Oplog archives fetching is canceled")
					return
				}
			}
			buf.Reset()
			if !firstFound { // TODO: do we need this check, add skip flag
				errc <- fmt.Errorf("'from' timestamp '%s' was not found in first archive: %s", from, arch.Filename())
				return
			}
		}
		errc <- fmt.Errorf("restore sequence was fetched, but restore point '%s' is not reached",
			until)
	}()

	return data, errc, nil
}
