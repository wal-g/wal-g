package binary

import (
	"context"
	"fmt"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"golang.org/x/sync/errgroup"
)

const mongoDisconnectTimeout = 30 * time.Second

func RunOplogReplay(ctx context.Context, mongodbURL string, replayArgs ReplyOplogConfig) error {
	if replayArgs.MinimalConfigPath != "" {
		return runSupervisedOplogReplay(ctx, replayArgs)
	}
	progress := stages.NewReplayProgress(replayArgs.Since)
	return runOplogReplay(ctx, mongodbURL, replayArgs, oplogReplayAttempt{
		checkpoint: progress.Snapshot(),
		progress:   progress,
	})
}

func runOplogReplay(
	ctx context.Context,
	mongodbURL string,
	replayArgs ReplyOplogConfig,
	attempt oplogReplayAttempt,
) (err error) {
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
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), mongoDisconnectTimeout)
		defer cancel()
		if closeErr := mongoClient.Close(closeCtx, false); err == nil && closeErr != nil {
			err = fmt.Errorf("can not close mongodb client: %w", closeErr)
		}
	}()

	since := attempt.checkpoint.OpTime.TS
	var emptyTS models.Timestamp
	if since == emptyTS {
		if replayArgs.WithCatchUpReconfig {
			since, err = mongoClient.CatchUpStartTS(ctx)
		} else {
			since, err = mongoClient.LastOplogTS(ctx)
		}
		if err != nil {
			return err
		}
	}
	attempt.progress.Initialize(since)

	if err = mongoClient.EnsureIsMaster(ctx); err != nil {
		return err
	}

	dbApplier := oplog.NewDBApplier(mongoClient, oplog.DBApplierArgs{
		// Catch-up replays onto an existing replica synced from master, so collection
		// UUIDs already match. Preserving 'ui' keeps them aligned through replay so
		// the replica can rejoin replSet without NamespaceNotFound on master's UUIDs
		PreserveUUID:   replayArgs.WithCatchUpReconfig,
		Partial:        replayArgs.Partial,
		Reconfig:       replayArgs.WithCatchUpReconfig,
		IgnoreErrCodes: replayArgs.IgnoreErrCodes,
	})
	oplogApplier := stages.NewCheckpointingApplier(
		mongoClient,
		dbApplier,
		replayArgs.FsyncInterval,
		attempt.progress,
	)

	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(ctx, archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}

	path, err := resolveOplogReplaySequence(ctx, downloader, since, replayArgs.Until)
	if err != nil {
		return err
	}

	// setup storage fetcher
	oplogFetcher := stages.NewStorageFetcher(downloader, path)
	if attempt.resumeAfter {
		oplogFetcher.WithResumeAfter()
	}

	// run worker cycle
	if err = HandleOplogReplay(ctx, since, replayArgs.Until, oplogFetcher, oplogApplier); err != nil {
		return err
	}
	if err = finalizeCatchUp(ctx, replayArgs, mongoClient, attempt.progress); err != nil {
		return err
	}
	return nil
}

func finalizeCatchUp(
	ctx context.Context,
	replayArgs ReplyOplogConfig,
	db client.MongoDriver,
	progress *stages.ReplayProgress,
) error {
	if !replayArgs.WithCatchUpReconfig {
		return nil
	}
	checkpoint := progress.Snapshot()
	if checkpoint.Generation == 0 {
		return nil
	}
	return db.ChangeOplogLastTimestamp(ctx, checkpoint.OpTime)
}

func resolveOplogReplaySequence(
	ctx context.Context,
	downloader archive.Downloader,
	since, until models.Timestamp,
) (archive.Sequence, error) {
	// because of oplog archives are write every 30 second intervals, we need to expand segment
	sinceStr := fmt.Sprintf("%s_%s", models.ArchiveTypeOplog, models.Timestamp{TS: since.TS - 300, Inc: 0}.String())
	untilStr := fmt.Sprintf("%s_%s", models.ArchiveTypeOplog, models.Timestamp{TS: until.TS + 30, Inc: until.Inc}.String())

	archives, err := downloader.ListOplogArchivesSegment(ctx, &sinceStr, &untilStr)
	if err != nil {
		return nil, err
	}
	path, err := archive.SequenceBetweenTS(archives, since, until)
	// if the start and end found in the archives, return the sequence
	if err == nil {
		return path, nil
	}

	// fallback to list all archives
	tracelog.WarningLogger.Println("fallback to ListFolder to find the last record", err)
	archives, err = downloader.ListOplogArchives(ctx)
	if err != nil {
		return nil, err
	}
	return archive.SequenceBetweenTS(archives, since, until)
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
		errgrp.Go(func() error {
			return <-errc
		})
	}

	return errgrp.Wait()
}
