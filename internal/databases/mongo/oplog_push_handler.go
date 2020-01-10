package mongo

import (
	"context"
	"sync"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/utility"
)

// HandleOplogPush starts oplog archiving process: fetch, validate, upload to storage.
// TODO: unit tests
// TODO: fetch only majority records
func HandleOplogPush(ctx context.Context, since oplog.Timestamp, fetcher oplog.FromFetcher, validator oplog.Validator, applier oplog.Applier) error {
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
	}()

	var errs []<-chan error
	oplogc, errc, err := fetcher.OplogFrom(ctx, since, wg)
	tracelog.ErrorLogger.FatalOnError(err)
	errs = append(errs, errc)

	validc, errc, err := validator.Validate(ctx, oplogc, wg)
	tracelog.ErrorLogger.FatalOnError(err)
	errs = append(errs, errc)

	errc, err = applier.Apply(ctx, validc, wg)
	tracelog.ErrorLogger.FatalOnError(err)
	errs = append(errs, errc)

	return utility.WaitFirstError(errs...)
}
