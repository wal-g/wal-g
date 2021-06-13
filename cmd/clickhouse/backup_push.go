package clickhouse

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/clickhouse"
	"github.com/wal-g/wal-g/utility"
)

const (
	backupPushShortDescription = "Pushes backup to storage"
	PermanentFlag              = "permanent"
	PermanentShorthand         = "p"
)

// backupPushCmd represents the backupPush command
var (
	backupPushCmd = &cobra.Command{
		Use:   "backup-push",
		Short: backupPushShortDescription,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
			defer func() { _ = signalHandler.Close() }()

			uploader, err := internal.ConfigureUploader()
			tracelog.ErrorLogger.FatalOnError(err)

			backupCmd, err := internal.GetCommandSetting(internal.ClickHouseCreateBackup)
			tracelog.ErrorLogger.FatalOnError(err)
			clickhouse.HandleBackupPush(uploader, backupCmd, permanent)
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			internal.RequiredSettings[internal.ClickHouseCreateBackup] = true
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
	},
}
	permanent = false
)

func init() {
	backupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes backup with 'permanent' flag")
	cmd.AddCommand(backupPushCmd)
}
