package oplog

import (
	"context"
	"fmt"
	"sync"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/tracelog"
)

// Applier defines interface to apply given oplog records.
type Applier interface {
	Apply(context.Context, chan Record, *sync.WaitGroup) (chan error, error)
}

// DBApplier implements Applier interface for mongodb
type DBApplier struct {
	uri string
	mc  *internal.MongoClient
}

// NewDBApplier builds DBApplier with given args.
func NewDBApplier(uri string) *DBApplier {
	return &DBApplier{uri: uri}
}

// Apply runs working cycle that applies oplog records.
func (dba *DBApplier) Apply(ctx context.Context, ch chan Record, wg *sync.WaitGroup) (chan error, error) {
	mc, err := internal.NewMongoClient(ctx, dba.uri)
	if err != nil {
		return nil, err
	}
	if _, err := mc.GetOplogCollection(ctx); err != nil {
		return nil, err
	}

	errc := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer mc.Close(ctx)
		defer close(errc)

		for op := range ch {
			tracelog.DebugLogger.Printf("Applyer receieved op %s (%s on %s)", op.TS, op.OP, op.NS)
			if err := mc.ApplyOp(ctx, op.Data); err != nil {
				errc <- fmt.Errorf("apply op (%s %s on %s) failed with: %w", op.TS, op.OP, op.NS, err)
				return
			}
		}
	}()

	return errc, nil
}
