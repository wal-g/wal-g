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
	Args:  cobra.NoArgs,
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

		uplProvider, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		uplProvider.UploadingFolder = uplProvider.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

		backupCmd, err := internal.GetCommandSettingContext(ctx, internal.NameStreamCreateCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		backupCmd.Stderr = os.Stderr
		uploader := archive.NewStorageUploader(uplProvider, uplProvider.UploadingFolder)
		err = mongo.HandleBackupPush(uploader, metaProvider, backupCmd)
		tracelog.ErrorLogger.FatalfOnError("Backup creation failed: %v", err)
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		internal.RequiredSettings[internal.NameStreamCreateCmd] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(backupPushCmd)
}
