package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/internal/databases/mongo/storage"
	"github.com/wal-g/wal-g/utility"
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

		mongodbUrl, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
		tracelog.ErrorLogger.FatalOnError(err)
		oplogFetcher := oplog.NewDBFetcher(mongodbUrl)

		uploader, err := storage.NewUploader(oplog.ArchBasePath)
		tracelog.ErrorLogger.FatalOnError(err)

		// discover last archived timestamp
		since, initial, err := oplog.ArchivingResumeTS(uploader.UploadingFolder)
		if initial {
			tracelog.InfoLogger.Printf("Initiating archiving first run")
		}
		tracelog.InfoLogger.Printf("Archiving last known timestamp is %s", since)
		oplogValidator := oplog.NewDBValidator(since)

		// set up archiving settings
		archiveAfterSize, err := internal.GetOplogArchiveAfterSize()
		tracelog.ErrorLogger.FatalOnError(err)
		archiveTimeout, err := internal.GetOplogArchiveTimeout()
		tracelog.ErrorLogger.FatalOnError(err)

		oplogApplier := oplog.NewStorageApplier(uploader, archiveAfterSize, archiveTimeout)
		err = mongo.HandleOplogPush(ctx, since, oplogFetcher, oplogValidator, oplogApplier)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(oplogPushCmd)
}
