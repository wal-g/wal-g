package mongo

import (
	"github.com/spf13/cobra"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const BackupListShortDescription = "Prints available backups"

// backupListCmd represents the backupList command
var backupListCmd = &cobra.Command{
	Use:   "backup-list",
	Short: BackupListShortDescription, // TODO : improve description
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		internal.HandleBackupList(folder)
	},
}

func init() {
	Cmd.AddCommand(backupListCmd)
}
