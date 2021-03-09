package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const backupFetchShortDescription = "Fetches desired backup from storage"

// backupFetchCmd represents the streamFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: backupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		internal.RequiredSettings[internal.NameStreamRestoreCmd] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		restoreCmd, err := internal.GetCommandSetting(internal.NameStreamRestoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		prepareCmd, _ := internal.GetCommandSetting(internal.MysqlBackupPrepareCmd)
		targetBackupSelector, err := internal.NewBackupNameSelector(args[0])
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleBackupFetch(folder, targetBackupSelector, restoreCmd, prepareCmd)
	},
}

func init() {
	cmd.AddCommand(backupFetchCmd)
}
