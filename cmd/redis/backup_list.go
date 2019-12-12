package redis

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const BackupListShortDescription = "Print available backups"

// backupListCmd represents the backupList command
var backupListCmd = &cobra.Command{
	Use:   "backup-list",
	Short: BackupListShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		internal.DefaultHandleBackupList(folder)
	},
}

func init() {
	Cmd.AddCommand(backupListCmd)
}
