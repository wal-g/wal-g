package sqlserver

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
	"github.com/wal-g/wal-g/utility"
)

const logRestoreShortDescription = "Restores log from the storage"

var logRestoreBackupName string
var logRestoreUntilTs string
var logRestoreDatabases []string
var logRestoreFrom []string
var logRestoreNoRecovery bool

var logRestoreCmd = &cobra.Command{
	Use:   "log-restore log-name",
	Short: logRestoreShortDescription,
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleLogRestore(logRestoreBackupName, logRestoreUntilTs, logRestoreDatabases, logRestoreFrom, logRestoreNoRecovery)
	},
}

func init() {
	logRestoreCmd.PersistentFlags().StringVar(&logRestoreBackupName, "since", "LATEST",
		"backup name starting from which you want to restore logs")
	logRestoreCmd.PersistentFlags().StringVar(&logRestoreUntilTs, "until",
		utility.TimeNowCrossPlatformUTC().Format(time.RFC3339), "time in RFC3339 for PITR")
	logRestoreCmd.PersistentFlags().StringSliceVarP(&logRestoreDatabases, "databases", "d", []string{},
		"List of databases to restore logs. All non-system databases from backup as default")
	logRestoreCmd.PersistentFlags().StringSliceVarP(&logRestoreFrom, "from", "f", []string{},
		"List of source database to restore logs from. By default it's the same as list of database, "+
			"those every database log is restored from self backup")
	logRestoreCmd.PersistentFlags().BoolVarP(&logRestoreNoRecovery, "no-recovery", "n", false,
		"Restore with NO_RECOVERY option")
	cmd.AddCommand(logRestoreCmd)
}
