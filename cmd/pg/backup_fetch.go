package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

const (
	BackupFetchShortDescription = "Fetches a backup from storage"
	MaskFlagDescription         = `Fetches only files which path relative to destination_directory
matches given shell file pattern.
For information about pattern syntax view: https://golang.org/pkg/path/filepath/#Match`
)

var fileMask string

// backupFetchCmd represents the backupFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch destination_directory backup_name",
	Short: BackupFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		internal.HandleBackupFetch(folder, args[0], args[1], fileMask)
	},
}

func init() {
	backupFetchCmd.Flags().StringVar(&fileMask, "mask", "", MaskFlagDescription)

	PgCmd.AddCommand(backupFetchCmd)
}
