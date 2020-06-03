package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const proxyShortDescription = "Run local azure blob emulator"

// proxyCmd represents the streamFetch command
var proxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: proxyShortDescription,
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		sqlserver.RunProxy(folder)
	},
}

func init() {
	Cmd.AddCommand(proxyCmd)
}
