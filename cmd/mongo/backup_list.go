package mongo

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
)

const BackupListShortDescription = "Prints available backups"

var verbose bool

// backupListCmd represents the backupList command
var backupListCmd = &cobra.Command{
	Use:   "backup-list",
	Short: BackupListShortDescription, // TODO : improve description
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
		tracelog.ErrorLogger.FatalOnError(err)
		listing := archive.NewDefaultTabbedBackupListing()
		err = mongo.HandleBackupsList(downloader, listing, os.Stdout, verbose)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(backupListCmd)
	backupListCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Verbose mode")
}
