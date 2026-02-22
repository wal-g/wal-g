package fdb

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/fdb"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/utility"
)

const backupPushShortDescription = "Pushes backup to storage"

// backupPushCmd represents the backupPush command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		uploader, err := internal.ConfigureUploader()
		logging.FatalOnError(err)
		uploader.ChangeDirectory(utility.BaseBackupPath)

		backupCmd, err := internal.GetCommandSetting(conf.NameStreamCreateCmd)
		logging.FatalOnError(err)
		fdb.HandleBackupPush(uploader, backupCmd)
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		conf.RequiredSettings[conf.NameStreamCreateCmd] = true
		err := internal.AssertRequiredSettingsSet()
		logging.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(backupPushCmd)
}
