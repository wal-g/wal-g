package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	CatchupFetchShortDescription = "Fetches an incremental backup from storage"
	UseNewUnwrapDescription      = "Use the new implementation of catchup unwrap (beta)"
)

var useNewUnwrap bool

// catchupFetchCmd represents the catchup-fetch command
var catchupFetchCmd = &cobra.Command{
	Use:   "catchup-fetch PGDATA backup_name",
	Short: CatchupFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		postgres.HandleCatchupFetch(folder, args[0], args[1], useNewUnwrap)
	},
}

func init() {
	catchupFetchCmd.Flags().BoolVar(&useNewUnwrap, "use-new-unwrap",
		false, UseNewUnwrapDescription)
	cmd.AddCommand(catchupFetchCmd)
}
