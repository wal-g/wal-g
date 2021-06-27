package rocksdb

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const backupFetchShortDescription = "Restores backup to database"

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch database_path backup_name",
	Short: backupFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactValidArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		_, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	backupFetchCmd.Flags().StringVar(&walDirectory, walDirectoryFlag, "", walDirectoryDescription)
	cmd.AddCommand(backupFetchCmd)
}
