package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
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
		archiveTimeout, err := internal.GetOplogArchiveTimeout()
		tracelog.ErrorLogger.FatalOnError(err)

		mongodbUrl, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
		tracelog.ErrorLogger.FatalOnError(err)

		// set up storage client
		uplProvider, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		uplProvider.UploadingFolder = uplProvider.UploadingFolder.GetSubFolder(models.OplogArchBasePath)
		uploader := archive.NewStorageUploader(uplProvider)

		// set up mongodb client and oplog fetcher
		mongoClient, err := client.NewMongoClient(ctx, mongodbUrl)
		tracelog.ErrorLogger.FatalOnError(err)
		err = mongoClient.EnsureIsMaster(ctx)
		tracelog.ErrorLogger.FatalOnError(err)

		// discover last archived timestamp
		since, initial, err := archive.ArchivingResumeTS(uplProvider.UploadingFolder)
		if initial {
			tracelog.InfoLogger.Printf("Initiating archiving first run")
			_, since, err = mongoClient.LastWriteTS(ctx)
			tracelog.ErrorLogger.FatalOnError(err)
		}
		tracelog.InfoLogger.Printf("Archiving last known timestamp is %s", since)

		/* File buffer is useful for debugging:
		fileBatchBuffer, err := stages.NewFileBuffer("/run/wal-g-oplog-push")
		tracelog.ErrorLogger.FatalOnError(err)
		defer tracelog.ErrorLogger.FatalOnError(fileBatchBuffer.Close())
		*/

		memoryBatchBuffer := stages.NewMemoryBuffer()
		defer tracelog.ErrorLogger.FatalOnError(memoryBatchBuffer.Close())
		// set up storage archiver
		oplogApplier := stages.NewStorageApplier(uploader, memoryBatchBuffer, archiveAfterSize, archiveTimeout)

		lwUpdate, err := internal.GetLastWriteUpdateInterval()
		tracelog.ErrorLogger.FatalOnError(err)

		oplogCursor, err := mongoClient.TailOplogFrom(ctx, since)
		tracelog.ErrorLogger.FatalOnError(err)

		oplogFetcher := stages.NewDBFetcher(mongoClient, oplogCursor, lwUpdate, stages.NewStorageGapHandler(uploader))

		// run working cycle
		err = mongo.HandleOplogPush(ctx, since, oplogFetcher, oplogApplier)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(oplogPushCmd)
}
