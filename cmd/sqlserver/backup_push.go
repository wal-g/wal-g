package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const backupPushShortDescription = "Creates new backup and pushes it to storage"

var backupPushDatabases []string

var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleBackupPush(backupPushDatabases)
	},
}

func init() {
	backupPushCmd.PersistentFlags().StringSliceVarP(&backupPushDatabases, "databases", "d", []string{},
		"List of databases to backup. All not-system databases as default")
	Cmd.AddCommand(backupPushCmd)
}
