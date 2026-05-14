package mongo

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
)

const backupFetchShortDescription = "Fetches desired backup from storage"

// backupFetchCmd represents the streamFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: backupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		storage, err := internal.ConfigureStorage()
		tracelog.ErrorLogger.FatalOnError(err)

		restoreCmd, err := internal.GetCommandSettingContext(cmd.Context(), conf.NameStreamRestoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		restoreCmd.Stdout = os.Stdout
		restoreCmd.Stderr = os.Stderr

		backupSelector, err := internal.NewBackupNameSelector(args[0], true)
		tracelog.ErrorLogger.FatalOnError(err)

		internal.HandleBackupFetch(storage.RootFolder(), backupSelector, internal.GetBackupToCommandFetcher(restoreCmd))
	},
}

func init() {
	cmd.AddCommand(backupFetchCmd)
}
