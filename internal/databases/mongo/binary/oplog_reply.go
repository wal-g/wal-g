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

const inlineMongodShutdownTimeout = 30 * time.Second

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

	initMongo := replayArgs.MinimalConfigPath != ""
	if initMongo {
		mongodProcess, err := Mongod(replayArgs.MinimalConfigPath).Start()
		if err != nil {
			return err
		}
		// Wait for inline mongod to actually exit before unwind: applier.Close
		// issues graceful `shutdown` admin command on happy path (db.Close with
		// initMongo=true), but Close() alone only SIGKILLs and races WiredTiger
		// checkpoint, leaving mongod.lock that breaks subsequent restart (e.g.
		// chown + supervisorctl in catch_up_stale_replica.feature). SIGKILL
		// fallback covers early-error paths where shutdown is never sent.
		defer func() {
			done := make(chan struct{})
			go func() {
				_ = mongodProcess.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(inlineMongodShutdownTimeout):
				tracelog.WarningLogger.Printf("inline mongod did not exit gracefully within %s, killing", inlineMongodShutdownTimeout)
				mongodProcess.Close()
				<-done
			}
		}()
		mongodbURL = mongodProcess.GetURI()
	}

	mongoClient, err := client.NewMongoClient(ctx, mongodbURL, mongoClientArgs...)
	if err != nil {
		return err
	}

	var emptyTS models.Timestamp
	if replayArgs.Since == emptyTS {
		replayArgs.Since, err = mongoClient.LastOplogTS(ctx)
		if err != nil {
			return err
		}
	}

	if err = mongoClient.EnsureIsMaster(ctx); err != nil {
		return err
	}

	dbApplier := oplog.NewDBApplier(mongoClient, oplog.DBApplierArgs{
		PreserveUUID:   false,
		Partial:        replayArgs.Partial,
		InitMongo:      initMongo,
		Reconfig:       replayArgs.WithCatchUpReconfig,
		IgnoreErrCodes: replayArgs.IgnoreErrCodes,
	})
	oplogApplier := stages.NewGenericApplier(dbApplier)

	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}

	path, err := resolveOplogReplaySequence(downloader, replayArgs.Since, replayArgs.Until)
	if err != nil {
		return err
	}

	// setup storage fetcher
	oplogFetcher := stages.NewStorageFetcher(downloader, path)

	// run worker cycle
	return HandleOplogReplay(ctx, replayArgs.Since, replayArgs.Until, oplogFetcher, oplogApplier)
}

func resolveOplogReplaySequence(
	downloader archive.Downloader,
	since, until models.Timestamp,
) (archive.Sequence, error) {
	// because of oplog archives are write every 30 second intervals, we need to expand segment
	sinceStr := fmt.Sprintf("%s_%s", models.ArchiveTypeOplog, models.Timestamp{TS: since.TS - 300, Inc: 0}.String())
	untilStr := fmt.Sprintf("%s_%s", models.ArchiveTypeOplog, models.Timestamp{TS: until.TS + 30, Inc: until.Inc}.String())

	archives, err := downloader.ListOplogArchivesSegment(&sinceStr, &untilStr)
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
	archives, err = downloader.ListOplogArchives()
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
