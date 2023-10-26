package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/wal-g/internal/daemon"
)

const DaemonShortDescription = "Runs WAL-G in daemon mode which executes commands sent from the lightweight walg-daemon-client."

// daemonCmd represents the daemon archive command
var daemonCmd = &cobra.Command{
	Use:   "daemon daemon_socket_path",
	Short: DaemonShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		daemonOpts := daemon.DaemonOptions{
			SocketPath: args[0],
		}
		daemon.HandleDaemon(daemonOpts, postgres.NewPostgreSQLDaemonListener())
	},
}

func init() {
	Cmd.AddCommand(daemonCmd)
}
