package mysql

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const BackupFetchShortDescription = "Fetches desired backup from storage"

// backupFetchCmd represents the streamFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: BackupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		internal.HandleBackupFetch(folder, args[0], internal.GetStreamFetcher(os.Stdout))
		if command, err := internal.GetStreamRestoreCmd(); err == nil {
			err := internal.ApplyCommand(command, nil)
			tracelog.ErrorLogger.FatalfOnError("failed to fetch backup due %v", err)
		}
	},
}

func init() {
	Cmd.AddCommand(backupFetchCmd)
}
