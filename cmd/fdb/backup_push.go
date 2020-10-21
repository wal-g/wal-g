package fdb

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/fdb"
	"github.com/wal-g/wal-g/utility"
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

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

		backupCmd, err := internal.GetCommandSetting(internal.NameStreamCreateCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		fdb.HandleBackupPush(uploader, backupCmd)
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		internal.RequiredSettings[internal.NameStreamCreateCmd] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(backupPushCmd)
}
