package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const backupListShortDescription = "Prints available backups"

// backupListCmd represents the backupList command
var backupListCmd = &cobra.Command{
	Use:   "backup-list",
	Short: backupListShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		// todo: implement pretty and json logic
		internal.DefaultHandleBackupList(folder.GetSubFolder(utility.BaseBackupPath), false, false)
	},
}

func init() {
	cmd.AddCommand(backupListCmd)
}
