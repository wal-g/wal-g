package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

const BackupFetchShortDescription = "Fetches a backup from storage"

// backupFetchCmd represents the backupFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch destination_directory backup_name",
	Short: BackupFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleBackupFetch(folder, args[0], args[1])
	},
}

func init() {
	RootCmd.AddCommand(backupFetchCmd)
}
