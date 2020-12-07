package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const backupRestoreShortDescription = "Restores backup from the storage"

var restoreDatabases []string
var restoreNoRecovery bool

var backupRestoreCmd = &cobra.Command{
	Use:   "backup-restore backup-name",
	Short: backupRestoreShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleBackupRestore(args[0], restoreDatabases, restoreNoRecovery)

	},
}

func init() {
	backupRestoreCmd.PersistentFlags().StringSliceVarP(&restoreDatabases, "databases", "d", []string{},
		"List of databases to restore. All non-system databases from backup as default")
	backupRestoreCmd.PersistentFlags().BoolVarP(&restoreNoRecovery, "no-recovery", "n", false,
		"Restore with NO_RECOVERY option")
	cmd.AddCommand(backupRestoreCmd)
}
