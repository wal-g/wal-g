package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const DaemonShortDescription = "Runs WAL-G in daemon mode which executes commands sent from the lightweight walg-daemon-client."

// daemonCmd represents the daemon archive command
var daemonCmd = &cobra.Command{
	Use:   "daemon daemon_socket_path",
	Short: DaemonShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := postgres.ConfigureMultiStorageFolder(true)
		tracelog.ErrorLogger.FatalfOnError("Failed to configure multi-storage folder: %v", err)

		walUploader, err := postgres.PrepareMultiStorageWalUploader(folder, targetStorage)
		tracelog.ErrorLogger.FatalOnError(err)

		folderReader, err := internal.PrepareMultiStorageFolderReader(folder, targetStorage)
		tracelog.ErrorLogger.FatalOnError(err)

		daemonOpts := postgres.DaemonOptions{
			Uploader: walUploader,
			Reader:   folderReader,
		}
		postgres.HandleDaemon(daemonOpts, args[0])
	},
}

func init() {
	Cmd.AddCommand(daemonCmd)
}
