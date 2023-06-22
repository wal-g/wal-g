package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const DaemonShortDescription = "Uploads a WAL file to storage"

// daemonCmd represents the daemon archive command
var daemonCmd = &cobra.Command{
	Use:   "daemon daemon_socket_path",
	Short: DaemonShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		daemonOpts := postgres.DaemonOptions{
			Uploader: GetWalUploader(),
			Reader:   GetFolderReader(),
		}
		postgres.HandleDaemon(daemonOpts, args[0])
	},
}

func init() {
	Cmd.AddCommand(daemonCmd)
}
