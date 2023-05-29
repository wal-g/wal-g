package mongo

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/discovery"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"github.com/wal-g/wal-g/internal/databases/mongo/stats"
	"github.com/wal-g/wal-g/internal/webserver"
	"github.com/wal-g/wal-g/utility"
)

// oplogPushCmd represents the continuous oplog archiving procedure
var oplogPushCmd = &cobra.Command{
	Use:   "oplog-push",
	Short: "Fetches oplog from mongodb and uploads to storage",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		defer func() { tracelog.ErrorLogger.FatalOnError(err) }()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		pushArgs, err := buildOplogPushRunArgs()
		if err != nil {
			return
		}

		statsArgs, err := buildOplogPushStatsArgs()
		if err != nil {
			return
		}

		err = runOplogPush(ctx, pushArgs, statsArgs)
	},
}

func init() {
	cmd.AddCommand(oplogPushCmd)
}

func runOplogPush(ctx context.Context, pushArgs oplogPushRunArgs, statsArgs oplogPushStatsArgs) error {
	// set up storage client
	tracelog.DebugLogger.Printf("starting oplog archiving with arguments: %+v", pushArgs)
	uplProvider, err := internal.ConfigureDefaultUploader()
	if err != nil {
		return err
	}
	uplProvider.ChangeDirectory(models.OplogArchBasePath)
	uploader := archive.NewStorageUploader(uplProvider)

	// set up mongodb client and oplog fetcher
	mongoClient, err := client.NewMongoClient(ctx, pushArgs.mongodbURL)
	if err != nil {
		return err
	}

	tracelog.DebugLogger.Printf("starting archiving stats with arguments: %+v", statsArgs)
	uploadStatsUpdater, err := configureUploadStatsUpdater(ctx, models.Timestamp{}, mongoClient, statsArgs)
	if err != nil {
		return err
	}

	if err = mongoClient.EnsureIsMaster(ctx); err != nil {
		if !pushArgs.primaryWait {
			return err
		}
		tracelog.InfoLogger.Printf("Archiving is waiting for mongodb to become a primary")
		if err = client.WaitForBecomePrimary(ctx, mongoClient, pushArgs.primaryWaitTimeout); err != nil {
			return err
		}
	}

	// Lookup for last timestamp archived to storage (set up storage downloader client)
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}
	since, err := discovery.ResolveStartingTS(ctx, downloader, mongoClient)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Archiving storage last known timestamp is %s", since)

	// fetch cursor started from since TS or from newest TS (if since is not exists)
	oplogCursor, since, err := discovery.BuildCursorFromTS(ctx, since, uploader, mongoClient)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Archiving is starting from timestamp %s", since)

	/* File buffer is useful for debugging:
	fileBatchBuffer, err := stages.NewFileBuffer("/run/wal-g-oplog-push")
	defer tracelog.ErrorLogger.PrintError(fileBatchBuffer.Close())
	*/

	memoryBatchBuffer := stages.NewMemoryBuffer()
	defer func() { tracelog.ErrorLogger.PrintOnError(memoryBatchBuffer.Close()) }()

	// set up storage archiver
	oplogApplier := stages.NewStorageApplier(uploader,
		memoryBatchBuffer,
		pushArgs.archiveAfterSize,
		pushArgs.archiveTimeout,
		uploadStatsUpdater)
	oplogFetcher := stages.NewCursorMajFetcher(mongoClient, oplogCursor, pushArgs.lwUpdate)

	// run working cycle
	return mongo.HandleOplogPush(ctx, oplogFetcher, oplogApplier)
}

type oplogPushRunArgs struct {
	archiveAfterSize   int
	archiveTimeout     time.Duration
	mongodbURL         string
	primaryWait        bool
	primaryWaitTimeout time.Duration
	lwUpdate           time.Duration
}

func buildOplogPushRunArgs() (args oplogPushRunArgs, err error) {
	// resolve archiving settings
	args.archiveAfterSize, err = internal.GetOplogArchiveAfterSize()
	if err != nil {
		return
	}
	args.archiveTimeout, err = internal.GetDurationSetting(internal.OplogArchiveTimeoutInterval)
	if err != nil {
		return
	}

	args.mongodbURL, err = internal.GetRequiredSetting(internal.MongoDBUriSetting)
	if err != nil {
		return
	}

	args.primaryWait, err = internal.GetBoolSettingDefault(internal.OplogPushWaitForBecomePrimary, false)
	if err != nil {
		return
	}

	if args.primaryWait {
		args.primaryWaitTimeout, err = internal.GetDurationSetting(internal.OplogPushPrimaryCheckInterval)
		if err != nil {
			return
		}
	}

	args.lwUpdate, err = internal.GetDurationSetting(internal.MongoDBLastWriteUpdateInterval)
	return
}

type oplogPushStatsArgs struct {
	enabled        bool
	updateInterval time.Duration
	logInterval    time.Duration
	exposeHTTP     bool
	httpPrefix     string
}

func buildOplogPushStatsArgs() (args oplogPushStatsArgs, err error) {
	args.enabled, err = internal.GetBoolSettingDefault(internal.OplogPushStatsEnabled, false)
	if err != nil || !args.enabled {
		return
	}

	args.updateInterval, err = internal.GetDurationSetting(internal.OplogPushStatsUpdateInterval)
	if err != nil {
		return
	}

	args.logInterval, err = internal.GetDurationSetting(internal.OplogPushStatsLoggingInterval)
	if err != nil {
		return
	}

	args.exposeHTTP, err = internal.GetBoolSettingDefault(internal.OplogPushStatsExposeHTTP, false)
	args.httpPrefix = stats.DefaultOplogPushStatsPrefix

	return
}

// configureUploadStatsUpdater starts statistics updates and exposes if configured
func configureUploadStatsUpdater(ctx context.Context,
	sinceTS models.Timestamp,
	mongoClient client.MongoDriver,
	args oplogPushStatsArgs) (stats.OplogUploadStatsUpdater, error) {
	if !args.enabled {
		return nil, nil
	}

	var opts []stats.OplogPushStatsOption
	if args.logInterval > 0 {
		opts = append(opts, stats.EnableLogReport(args.logInterval, tracelog.InfoLogger.Printf))
	}

	if args.exposeHTTP {
		opts = append(opts, stats.EnableHTTPHandler(args.httpPrefix, webserver.DefaultWebServer))
	}

	uploadStats := stats.NewOplogUploadStats(sinceTS)
	archivingStats := stats.NewOplogPushStats(ctx, uploadStats, mongoClient, opts...)
	if err := archivingStats.Update(); err != nil {
		return nil, err
	}
	go stats.RefreshWithInterval(ctx, args.updateInterval, archivingStats, tracelog.WarningLogger.Printf)

	return uploadStats, nil
}
