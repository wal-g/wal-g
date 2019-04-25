package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

const WalPrefetchShortDescription = `Used for prefetching process forking
and should not be called by user.`

// WalPrefetchCmd represents the walPrefetch command
var WalPrefetchCmd = &cobra.Command{
	Use:    "wal-prefetch wal_name prefetch_location",
	Short:  WalPrefetchShortDescription,
	Args:   cobra.ExactArgs(2),
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleWALPrefetch(uploader, args[0], args[1])
	},
}

func init() {
	PgCmd.AddCommand(WalPrefetchCmd)
}
