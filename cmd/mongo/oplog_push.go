package mongo

import (
	"context"
	"os"
	"syscall"

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

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

// oplogPushCmd represents the continuous oplog archiving procedure
var oplogPushCmd = &cobra.Command{
	Use:   "oplog-push",
	Short: "Fetches oplog from mongodb and uploads to storage",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		// resolve archiving settings
		archiveAfterSize, err := internal.GetOplogArchiveAfterSize()
		tracelog.ErrorLogger.FatalOnError(err)
		archiveTimeout, err := internal.GetDurationSetting(internal.OplogArchiveTimeoutInterval)
		tracelog.ErrorLogger.FatalOnError(err)

		mongodbUrl, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
		tracelog.ErrorLogger.FatalOnError(err)

		// set up storage client
		uplProvider, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		uplProvider.UploadingFolder = uplProvider.UploadingFolder.GetSubFolder(models.OplogArchBasePath)
		uploader := archive.NewStorageUploader(uplProvider, uplProvider.UploadingFolder)

		// set up mongodb client and oplog fetcher
		mongoClient, err := client.NewMongoClient(ctx, mongodbUrl)
		tracelog.ErrorLogger.FatalOnError(err)

		primaryWait, err := internal.GetBoolSetting(internal.OplogPushWaitForBecomePrimary, false)
		tracelog.ErrorLogger.FatalOnError(err)

		uploadStatsUpdater := HandleOplogPushStatistics(ctx, models.Timestamp{}, mongoClient)
		if err := mongoClient.EnsureIsMaster(ctx); err != nil {
			if !primaryWait {
				tracelog.ErrorLogger.FatalOnError(err)
			}
			primaryWaitTimeout, err := internal.GetDurationSetting(internal.OplogPushPrimaryCheckInterval)
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.ErrorLogger.FatalOnError(client.WaitForBecomePrimary(ctx, mongoClient, primaryWaitTimeout))
		}

		// Lookup for last timestamp archived to storage (set up storage downloader client)
		downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
		tracelog.ErrorLogger.FatalOnError(err)
		since, err := discovery.ResolveStartingTS(ctx, downloader, mongoClient)
		tracelog.InfoLogger.Printf("Archiving storage last known timestamp is %s", since)

		// fetch cursor started from since TS or from newest TS (if since is not exists)
		oplogCursor, since, err := discovery.BuildCursorFromTS(ctx, since, uploader, mongoClient)
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.InfoLogger.Printf("Archiving is starting from timestamp %s", since)

		/* File buffer is useful for debugging:
		fileBatchBuffer, err := stages.NewFileBuffer("/run/wal-g-oplog-push")
		tracelog.ErrorLogger.FatalOnError(err)
		defer tracelog.ErrorLogger.FatalOnError(fileBatchBuffer.Close())
		*/

		memoryBatchBuffer := stages.NewMemoryBuffer()
		defer tracelog.ErrorLogger.FatalOnError(memoryBatchBuffer.Close())

		// set up storage archiver
		oplogApplier := stages.NewStorageApplier(uploader, memoryBatchBuffer, archiveAfterSize, archiveTimeout, uploadStatsUpdater)

		lwUpdate, err := internal.GetDurationSetting(internal.MongoDBLastWriteUpdateInterval)
		tracelog.ErrorLogger.FatalOnError(err)

		oplogFetcher := stages.NewCursorMajFetcher(mongoClient, oplogCursor, lwUpdate)

		// run working cycle
		err = mongo.HandleOplogPush(ctx, oplogFetcher, oplogApplier)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

// HandleOplogPushStatistics starts statistics updates and exposes if configured
func HandleOplogPushStatistics(ctx context.Context, sinceTS models.Timestamp, mongoClient client.MongoDriver) stats.OplogUploadStatsUpdater {
	oplogPushStatsEnabled, err := internal.GetBoolSetting(internal.OplogPushStatsEnabled, false)
	tracelog.ErrorLogger.FatalOnError(err)
	if !oplogPushStatsEnabled {
		return nil
	}

	statsUpdateInterval, err := internal.GetDurationSetting(internal.OplogPushStatsUpdateInterval)
	tracelog.ErrorLogger.FatalOnError(err)

	var opts []stats.OplogPushStatsOption

	statsLogInterval, err := internal.GetDurationSetting(internal.OplogPushStatsLoggingInterval)
	tracelog.ErrorLogger.FatalOnError(err)
	if statsLogInterval > 0 {
		opts = append(opts, stats.EnableLogReport(statsLogInterval, tracelog.InfoLogger.Printf))
	}

	exposeHttp, err := internal.GetBoolSetting(internal.OplogPushStatsExposeHttp, false)
	tracelog.ErrorLogger.FatalOnError(err)
	if exposeHttp {
		opts = append(opts, stats.EnableHTTPHandler(stats.DefaultOplogPushStatsPrefix, webserver.DefaultWebServer))
	}

	uploadStats := stats.NewOplogUploadStats(sinceTS)
	archivingStats := stats.NewOplogPushStats(ctx, uploadStats, mongoClient, opts...)
	tracelog.ErrorLogger.FatalOnError(archivingStats.Update())
	go stats.RefreshWithInterval(ctx, statsUpdateInterval, archivingStats, tracelog.WarningLogger.Printf)

	return uploadStats
}

func init() {
	Cmd.AddCommand(oplogPushCmd)
}
