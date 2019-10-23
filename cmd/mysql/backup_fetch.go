package mysql

import (
	"github.com/spf13/cobra"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"os"
)

const BackupFetchShortDescription = "Fetches desired backup from storage"

// backupFetchCmd represents the streamFetch command
var backupFetchCmd = &cobra.Command {
	Use:   "backup-fetch backup-name",
	Short: BackupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		if val, err := internal.GetStreamRestoreCmd(); err == nil {
			waitFunc, stdout := internal.RestoreCommand(val)
			internal.HandleBackupFetch(folder, args[0], internal.GetStreamFetcher(stdout))
			waitFunc()
		} else {
			tracelog.InfoLogger.Print("Write stream to stdout")
			// variable command to restore from stream not configured - lets write to stdout
			internal.HandleBackupFetch(folder, args[0], internal.GetStreamFetcher(os.Stdout))
		}
	},
}

func init() {
	Cmd.AddCommand(backupFetchCmd)
}