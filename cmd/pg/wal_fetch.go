package pg

import (
	"github.com/spf13/cobra"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const WalFetchShortDescription = "Fetches a WAL file from storage"

// walFetchCmd represents the walFetch command
var walFetchCmd = &cobra.Command{
	Use:   "wal-fetch wal_name destination_filename",
	Short: WalFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		internal.HandleWALFetch(folder, args[0], args[1], true)
	},
}

func init() {
	Cmd.AddCommand(walFetchCmd)
}
