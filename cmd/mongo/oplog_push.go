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

	uplProvider, err := internal.ConfigureUploader()
	if err != nil {
		return err
	}
	uplProvider.UploadingFolder = uplProvider.UploadingFolder.GetSubFolder(models.OplogArchBasePath)
	uploader := archive.NewStorageUploader(uplProvider)

	// set up mongodb client and oplog fetcher
	mongoClient, err := client.NewMongoClient(ctx, pushArgs.mongodbUrl)
	if err != nil {
		return err
	}

	uploadStatsUpdater, err := configureUploadStatsUpdater(ctx, models.Timestamp{}, mongoClient, statsArgs)
	if err != nil {
		return err
	}

	if err = mongoClient.EnsureIsMaster(ctx); err != nil {
		if !pushArgs.primaryWait {
			return err
		}
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
	defer tracelog.ErrorLogger.PrintError(memoryBatchBuffer.Close())

	// set up storage archiver
	oplogApplier := stages.NewStorageApplier(uploader, memoryBatchBuffer, pushArgs.archiveAfterSize, pushArgs.archiveTimeout, uploadStatsUpdater)
	oplogFetcher := stages.NewCursorMajFetcher(mongoClient, oplogCursor, pushArgs.lwUpdate)

	// run working cycle
	return mongo.HandleOplogPush(ctx, oplogFetcher, oplogApplier)
}

type oplogPushRunArgs struct {
	archiveAfterSize   int
	archiveTimeout     time.Duration
	mongodbUrl         string
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

	args.mongodbUrl, err = internal.GetRequiredSetting(internal.MongoDBUriSetting)
	if err != nil {
		return
	}

	primaryWait, err := internal.GetBoolSettingDefault(internal.OplogPushWaitForBecomePrimary, false)
	if err != nil {
		return
	}

	if primaryWait {
		args.primaryWaitTimeout, err = internal.GetDurationSetting(internal.OplogPushPrimaryCheckInterval)
		if err != nil {
			return
		}
	}

	args.lwUpdate, err = internal.GetDurationSetting(internal.MongoDBLastWriteUpdateInterval)
	return
}

type oplogPushStatsArgs struct {
	Enabled        bool
	UpdateInterval time.Duration
	LogInterval    time.Duration
	Options        []stats.OplogPushStatsOption
	ExposeHTTP     bool
	HTTPPrefix     string
}

func buildOplogPushStatsArgs() (args oplogPushStatsArgs, err error) {
	args.Enabled, err = internal.GetBoolSettingDefault(internal.OplogPushStatsEnabled, false)
	if err != nil || !args.Enabled {
		return
	}

	args.UpdateInterval, err = internal.GetDurationSetting(internal.OplogPushStatsUpdateInterval)
	if err != nil {
		return
	}

	args.LogInterval, err = internal.GetDurationSetting(internal.OplogPushStatsLoggingInterval)
	if err != nil {
		return
	}

	args.ExposeHTTP, err = internal.GetBoolSettingDefault(internal.OplogPushStatsExposeHttp, false)
	args.HTTPPrefix = stats.DefaultOplogPushStatsPrefix

	return
}

// configureUploadStatsUpdater starts statistics updates and exposes if configured
func configureUploadStatsUpdater(ctx context.Context, sinceTS models.Timestamp, mongoClient client.MongoDriver, args oplogPushStatsArgs) (stats.OplogUploadStatsUpdater, error) {
	if !args.Enabled {
		return nil, nil
	}

	var opts []stats.OplogPushStatsOption
	if args.LogInterval > 0 {
		opts = append(opts, stats.EnableLogReport(args.LogInterval, tracelog.InfoLogger.Printf))
	}

	if args.ExposeHTTP {
		opts = append(opts, stats.EnableHTTPHandler(args.HTTPPrefix, webserver.DefaultWebServer))
	}

	uploadStats := stats.NewOplogUploadStats(sinceTS)
	archivingStats := stats.NewOplogPushStats(uploadStats, mongoClient, args.Options...)
	if err := archivingStats.Update(); err != nil {
		return nil, err
	}
	go stats.RefreshWithInterval(ctx, args.UpdateInterval, archivingStats, tracelog.WarningLogger.Printf)

	return uploadStats, nil
}
