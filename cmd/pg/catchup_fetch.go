package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	CatchupFetchShortDescription = "Fetches an incremental backup from storage"
)

// catchupFetchCmd represents the catchup-fetch command
var catchupFetchCmd = &cobra.Command{
	Use:   "catchup-fetch PGDATA backup_name",
	Short: CatchupFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		internal.HandleCatchupFetch(folder, args[0], args[1])
	},
}

func init() {
	Cmd.AddCommand(catchupFetchCmd)
}
