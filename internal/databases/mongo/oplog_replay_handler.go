package mongo

import (
	"context"
	"sync"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/utility"
)

// HandleOplogReplay starts oplog replay process: download from storage and apply to mongodb
// TODO: unit tests
func HandleOplogReplay(ctx context.Context, since, until models.Timestamp, fetcher oplog.BetweenFetcher, applier oplog.Applier) error {
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
	}()

	var errs []<-chan error
	oplogc, errc, err := fetcher.OplogBetween(ctx, since, until, wg)
	if err != nil {
		return err
	}
	errs = append(errs, errc)

	errc, err = applier.Apply(ctx, oplogc, wg)
	if err != nil {
		return err
	}
	errs = append(errs, errc)

	return utility.WaitFirstError(errs...)
}
