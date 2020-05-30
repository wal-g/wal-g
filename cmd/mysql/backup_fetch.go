package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	backupFetchShortDescription = "Fetches desired backup from storage"

	XtrabackupApplyLogOnly   = "--apply-log-only"
	XtrabackupIncrementalDir = "--incremental-dir"
)

// backupFetchCmd represents the streamFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: backupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		restoreCmd, err := internal.GetCommandSetting(internal.NameStreamRestoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		prepareCmd, _ := internal.GetCommandSetting(internal.MysqlBackupPrepareCmd)
		mysql.HandleBackupFetch(folder, args[0], restoreCmd, prepareCmd)
	},
}

func init() {
	Cmd.AddCommand(backupFetchCmd)
}
