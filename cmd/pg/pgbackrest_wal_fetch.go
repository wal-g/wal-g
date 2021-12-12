package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/pgbackrest"
)

var pgbackrestWalFetchCmd = &cobra.Command{
	Use:   "wal-fetch wal_name destination_filename",
	Short: WalFetchShortDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, stanza := configurePgbackrestSettings()
		pgbackrest.HandleWalFetch(folder, stanza, args[0], args[1])
	},
}

func init() {
	pgbackrestCmd.AddCommand(walFetchCmd)
}