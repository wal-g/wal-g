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
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

const oplogPushShortDescription = ""

// oplogPushCmd represents the continuous oplog archiving procedure
var oplogPushCmd = &cobra.Command{
	Use:   "oplog-push",
	Short: oplogPushShortDescription,
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

		// set up mongodb client and oplog fetcher
		mongoClient, err := client.NewMongoClient(ctx, mongodbUrl)
		tracelog.ErrorLogger.FatalOnError(err)
		oplogFetcher := oplog.NewDBFetcher(mongoClient)

		// set up storage client
		uploader, err := archive.NewStorageUploader(models.ArchBasePath)
		tracelog.ErrorLogger.FatalOnError(err)

		// discover last archived timestamp
		since, initial, err := archive.ArchivingResumeTS(uploader.UploadingFolder)
		if initial {
			tracelog.InfoLogger.Printf("Initiating archiving first run")
			_, since, err = mongoClient.LastWriteTS(ctx)
			tracelog.ErrorLogger.FatalOnError(err)
		}
		tracelog.InfoLogger.Printf("Archiving last known timestamp is %s", since)

		// set up oplog validator
		oplogValidator := oplog.NewDBValidator(since)

		// set up storage archiver
		oplogApplier := oplog.NewStorageApplier(uploader, archiveAfterSize, archiveTimeout)

		// run working cycle
		err = mongo.HandleOplogPush(ctx, since, oplogFetcher, oplogValidator, oplogApplier)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(oplogPushCmd)
}
