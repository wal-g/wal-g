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
	mongoClient, err := newOplogReplayMongoClient(ctx, mongodbURL, replayArgs)
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

	since, err := resolveOplogReplayStart(ctx, mongoClient, replayArgs, attempt.checkpoint)
	if err != nil {
		return err
	}
	attempt.progress.Initialize(since)
	attempt.progress.BeginAttempt()

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
		replayArgs.ApplyBatchSize,
		replayArgs.WithCatchUpReconfig,
	)

	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(ctx, archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}

	if err = runOplogReplayPipeline(
		ctx, replayArgs, attempt, downloader, oplogApplier, since); err != nil {
		return err
	}
	return nil
}

func newOplogReplayMongoClient(
	ctx context.Context,
	mongodbURL string,
	replayArgs ReplyOplogConfig,
) (*client.MongoClient, error) {
	var options []client.Option
	if replayArgs.OplogAlwaysUpsert != nil {
		options = append(options, client.OplogAlwaysUpsert(*replayArgs.OplogAlwaysUpsert))
	}
	if replayArgs.OplogApplicationMode != nil {
		options = append(options,
			client.OplogApplicationMode(client.OplogAppMode(*replayArgs.OplogApplicationMode)))
	}
	return client.NewMongoClient(ctx, mongodbURL, options...)
}

func resolveOplogReplayStart(
	ctx context.Context,
	db client.MongoDriver,
	replayArgs ReplyOplogConfig,
	checkpoint stages.ReplayCheckpoint,
) (models.Timestamp, error) {
	if checkpoint.OpTime.TS != (models.Timestamp{}) {
		return checkpoint.OpTime.TS, nil
	}
	if replayArgs.WithCatchUpReconfig {
		return db.CatchUpStartTS(ctx)
	}
	return db.LastOplogTS(ctx)
}

func runOplogReplayPipeline(
	ctx context.Context,
	replayArgs ReplyOplogConfig,
	attempt oplogReplayAttempt,
	downloader archive.Downloader,
	applier stages.Applier,
	since models.Timestamp,
) error {
	if replayArgs.FollowLatest != nil {
		fetcher := stages.NewFollowingStorageFetcher(downloader, replayArgs.FollowLatest.PollInterval)
		controller := newCatchUpController(*replayArgs.FollowLatest, attempt.progress, fetcher)
		return HandleFollowingOplogReplay(ctx, since, attempt.resumeAfter, fetcher, applier, controller)
	}

	path, err := resolveOplogReplaySequence(ctx, downloader, since, replayArgs.Until)
	if err != nil {
		return err
	}
	fetcher := stages.NewStorageFetcher(downloader, path)
	if attempt.resumeAfter {
		fetcher.WithResumeAfter()
	}
	return HandleOplogReplay(ctx, since, replayArgs.Until, fetcher, applier)
}

func HandleFollowingOplogReplay(
	ctx context.Context,
	since models.Timestamp,
	resumeAfter bool,
	fetcher *stages.FollowingStorageFetcher,
	applier stages.Applier,
	controller *catchUpController,
) error {
	errGroup, ctx := errgroup.WithContext(ctx)
	oplogC, fetchErrC, err := fetcher.FetchFrom(ctx, since, resumeAfter)
	if err != nil {
		return err
	}
	applyErrC, err := applier.Apply(ctx, oplogC)
	if err != nil {
		return err
	}

	errGroup.Go(func() error { return <-fetchErrC })
	errGroup.Go(func() error { return <-applyErrC })
	errGroup.Go(func() error { return controller.Run(ctx) })
	return errGroup.Wait()
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
