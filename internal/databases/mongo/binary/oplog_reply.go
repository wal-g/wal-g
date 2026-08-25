package binary

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"golang.org/x/sync/errgroup"
)

const (
	mongodExitDetectionTimeout = time.Second
)

func RunOplogReplay(ctx context.Context, mongodbURL string, replayArgs ReplyOplogConfig) error {
	if replayArgs.FsyncInterval <= 0 {
		replayArgs.FsyncInterval = 10 * time.Minute
	}
	if replayArgs.MinimalConfigPath != "" {
		return runInlineOplogReplay(ctx, replayArgs)
	}
	return runOplogReplay(ctx, mongodbURL, replayArgs)
}

func runOplogReplay(ctx context.Context, mongodbURL string, replayArgs ReplyOplogConfig) error {
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

	var emptyTS models.Timestamp
	if replayArgs.Since == emptyTS {
		if replayArgs.WithCatchUpReconfig {
			replayArgs.Since, err = mongoClient.CatchUpStartTS(ctx)
		} else {
			replayArgs.Since, err = mongoClient.LastOplogTS(ctx)
		}
		if err != nil {
			return err
		}
	}
	if replayArgs.progress == nil {
		replayArgs.progress = stages.NewReplayProgress(replayArgs.Since)
	} else {
		replayArgs.progress.Initialize(replayArgs.Since)
	}

	if err = mongoClient.EnsureIsMaster(ctx); err != nil {
		return err
	}

	dbApplier := oplog.NewDBApplier(mongoClient, oplog.DBApplierArgs{
		// Catch-up replays onto an existing replica synced from master, so collection
		// UUIDs already match. Preserving 'ui' keeps them aligned through replay so
		// the replica can rejoin replSet without NamespaceNotFound on master's UUIDs
		PreserveUUID:   replayArgs.WithCatchUpReconfig,
		Partial:        replayArgs.Partial,
		InitMongo:      false,
		Reconfig:       replayArgs.WithCatchUpReconfig,
		IgnoreErrCodes: replayArgs.IgnoreErrCodes,
	})
	oplogApplier := stages.NewCheckpointingApplier(
		mongoClient,
		dbApplier,
		replayArgs.FsyncInterval,
		replayArgs.progress,
	)

	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(ctx, archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}

	path, err := resolveOplogReplaySequence(ctx, downloader, replayArgs.Since, replayArgs.Until)
	if err != nil {
		return err
	}

	// setup storage fetcher
	oplogFetcher := stages.NewStorageFetcher(downloader, path)
	if replayArgs.resumeAfter {
		oplogFetcher.WithResumeAfter()
	}

	// run worker cycle
	return HandleOplogReplay(ctx, replayArgs.Since, replayArgs.Until, oplogFetcher, oplogApplier)
}

func runInlineOplogReplay(ctx context.Context, replayArgs ReplyOplogConfig) error {
	progress := stages.NewReplayProgress(replayArgs.Since)
	consecutiveRestarts := 0

	for {
		_, durableTS, generationBefore := progress.Snapshot()
		progress.ResetAttempt()

		attemptConfig := replayArgs
		attemptConfig.MinimalConfigPath = ""
		attemptConfig.Since = durableTS
		attemptConfig.progress = progress
		attemptConfig.resumeAfter = generationBefore > 0

		mongodCrashed, err := runInlineOplogReplayAttempt(ctx, attemptConfig, replayArgs.MinimalConfigPath)
		if err == nil {
			return nil
		}
		if !mongodCrashed {
			return err
		}

		_, durableTS, generationAfter := progress.Snapshot()
		if generationAfter > generationBefore {
			consecutiveRestarts = 0
		}
		if consecutiveRestarts >= replayArgs.MaxMongodRestarts {
			return errors.Wrapf(err,
				"inline mongod crashed after %d restarts, last durable oplog timestamp is %s",
				consecutiveRestarts, durableTS)
		}
		consecutiveRestarts++
		tracelog.WarningLogger.Printf(
			"inline mongod crashed, restarting oplog replay after %s, attempt %d/%d",
			durableTS, consecutiveRestarts, replayArgs.MaxMongodRestarts)
	}
}

func runInlineOplogReplayAttempt(
	ctx context.Context,
	replayArgs ReplyOplogConfig,
	minimalConfigPath string,
) (bool, error) {
	mongodProcess := Mongod(minimalConfigPath).
		WithParams(DisableLogicalSessionCacheRefresh, TakeUnstableCheckpointOnShutdown)
	if replayArgs.Partial {
		mongodProcess.WithRestore()
	}
	if _, err := mongodProcess.Start(ctx); err != nil {
		return false, errors.Wrap(err, "unable to start mongod in special mode")
	}

	waitC := make(chan error, 1)
	go func() { waitC <- mongodProcess.Wait() }()
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()

	mongodService, err := CreateMongodService(attemptCtx, "wal-g oplog replay", mongodProcess.GetURI(), 10*time.Minute)
	if err != nil {
		mongodProcess.Close()
		<-waitC
		return false, errors.Wrap(err, "unable to create mongod service")
	}

	replayC := make(chan error, 1)
	go func() { replayC <- runOplogReplay(attemptCtx, mongodProcess.GetURI(), replayArgs) }()

	select {
	case processErr := <-waitC:
		cancelAttempt()
		<-replayC
		return true, unexpectedMongodExit(processErr)

	case replayErr := <-replayC:
		if replayErr != nil {
			select {
			case processErr := <-waitC:
				return true, unexpectedMongodExit(processErr)
			// The driver may report a broken connection just before cmd.Wait publishes the process exit.
			case <-time.After(mongodExitDetectionTimeout):
				mongodProcess.Close()
				<-waitC
				return false, replayErr
			}
		}

		if err = mongodService.Shutdown(ctx); err != nil {
			mongodProcess.Close()
			<-waitC
			return false, err
		}
		return false, <-waitC
	}
}

func unexpectedMongodExit(err error) error {
	if err == nil {
		return fmt.Errorf("inline mongod exited unexpectedly during oplog replay")
	}
	return errors.Wrap(err, "inline mongod exited during oplog replay")
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
