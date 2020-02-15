package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

const BackupPushShortDescription = "Pushes backup to storage"

// backupPushCmd represents the backupPush command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: BackupPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		mongodbUrl, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
		tracelog.ErrorLogger.FatalOnError(err)

		// set up mongodb client and oplog fetcher
		mongoClient, err := client.NewMongoClient(ctx, mongodbUrl)
		tracelog.ErrorLogger.FatalOnError(err)
		metaProvider := archive.NewBackupMetaMongoProvider(ctx, mongoClient)

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

		command := internal.GetStreamCreateCmd()
		mongo.HandleStreamPush(&archive.StorageUploader{WalUploader: uploader}, command, metaProvider)
	},
}

func init() {
	Cmd.AddCommand(backupPushCmd)
}
