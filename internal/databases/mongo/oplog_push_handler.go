package mongo

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"golang.org/x/sync/errgroup"
)

// HandleOplogPush starts oplog archiving process: fetch, validate, upload to storage.
func HandleOplogPush(ctx context.Context, fetcher stages.Fetcher, applier stages.Applier) error {
	errgrp := errgroup.Group{}
	var errs []<-chan error

	oplogc, errc, err := fetcher.Fetch(ctx)
	if err != nil {
		return err
	}
	errs = append(errs, errc)

	errc, err = applier.Apply(ctx, oplogc)
	if err != nil {
		return err
	}
	errs = append(errs, errc)

	for _, errc := range errs {
		errc := errc
		errgrp.Go(func() error {
			return <-errc
		})
	}

	return errgrp.Wait()
}
