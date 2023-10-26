package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/common/daemon"
)

const DaemonShortDescription = "Runs WAL-G in daemon mode which executes commands sent from the lightweight walg-daemon-client."

// daemonCmd represents the daemon archive command
var daemonCmd = &cobra.Command{
	Use:   "daemon daemon_socket_path",
	Short: DaemonShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		daemonOpts := postgres.DaemonOptions{
			SocketPath: args[0],
		}
		daemon.HandleDaemon(daemonOpts)
	},
}

func init() {
	Cmd.AddCommand(daemonCmd)
}
