package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const backupPushShortDescription = "Creates new backup and pushes it to storage"

var backupPushDatabases []string
var backupCompression bool
var backupUpdateLatest bool

var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleBackupPush(backupPushDatabases, backupUpdateLatest, backupCompression)
	},
}

func init() {
	backupPushCmd.PersistentFlags().StringSliceVarP(&backupPushDatabases, "databases", "d", []string{},
		"List of databases to backup. All not-system databases as default")
	backupPushCmd.PersistentFlags().BoolVarP(&backupUpdateLatest, "update-latest", "u", false,
		"Update latest backup instead of creating new one")
	backupPushCmd.PersistentFlags().BoolVarP(&backupCompression, "compression", "c", true,
		"Use built-in backup compression. Enabled by default")
	Cmd.AddCommand(backupPushCmd)
}
