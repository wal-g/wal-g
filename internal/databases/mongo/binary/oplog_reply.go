package binary

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"golang.org/x/sync/errgroup"
)

func RunOplogReplay(ctx context.Context, mongodbURL string, replayArgs ReplyOplogConfig) error {
	// set up mongodb client and oplog applier
	var mongoClientArgs []client.Option
	if replayArgs.OplogAlwaysUpsert != nil {
		mongoClientArgs = append(mongoClientArgs, client.OplogAlwaysUpsert(*replayArgs.OplogAlwaysUpsert))
	}

	if replayArgs.OplogApplicationMode != nil {
		mongoClientArgs = append(mongoClientArgs,
			client.OplogApplicationMode(client.OplogAppMode(*replayArgs.OplogApplicationMode)))
	}

	mongoClient, err := client.NewMongoClient(ctx, mongodbURL, mongoClientArgs...)
	if err != nil {
		return err
	}

	if err = mongoClient.EnsureIsMaster(ctx); err != nil {
		return err
	}

	dbApplier := oplog.NewDBApplier(mongoClient, false, replayArgs.IgnoreErrCodes)
	oplogApplier := stages.NewGenericApplier(dbApplier)

	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}
	// discover archive sequence to replay
	archives, err := downloader.ListOplogArchives()
	if err != nil {
		return err
	}
	path, err := archive.SequenceBetweenTS(archives, replayArgs.Since, replayArgs.Until)
	if err != nil {
		return err
	}

	// setup storage fetcher
	oplogFetcher := stages.NewStorageFetcher(downloader, path)

	// run worker cycle
	return HandleOplogReplay(ctx, replayArgs.Since, replayArgs.Until, oplogFetcher, oplogApplier)
}

// HandleOplogReplay starts oplog replay process: download from storage and apply to mongodb
func HandleOplogReplay(ctx context.Context,
	since,
	until models.Timestamp,
	fetcher stages.BetweenFetcher,
	applier stages.Applier) error {
	errgrp, ctx := errgroup.WithContext(ctx)
	var errs []<-chan error

	oplogc, errc, err := fetcher.FetchBetween(ctx, since, until)
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
