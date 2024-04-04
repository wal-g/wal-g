package etcd

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/etcd"
	"github.com/wal-g/wal-g/utility"
)

const (
	backupPushShortDescription = "Creates new backup and pushes it to storage"
)

var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	PreRun: func(cmd *cobra.Command, args []string) {
		conf.RequiredSettings[conf.NameStreamCreateCmd] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		uploader.ChangeDirectory(utility.BaseBackupPath)

		backupCmd, err := internal.GetCommandSetting(conf.NameStreamCreateCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		etcd.HandleBackupPush(uploader, backupCmd)
	},
}

func init() {
	cmd.AddCommand(backupPushCmd)
}
