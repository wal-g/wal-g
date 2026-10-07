package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	catchupPushShortDescription = "Creates incremental backup from lsn"
)

var (
	// catchupPushCmd represents the catchup-push command
	catchupPushCmd = &cobra.Command{
		Use:   "catchup-push PGDATA --from-lsn LSN",
		Short: catchupPushShortDescription,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			lsn, err := postgres.ParseLSNOrNumber(fromLSN)
			tracelog.ErrorLogger.FatalOnError(err)

			internal.ConfigureLimiters()

			postgres.HandleCatchupPush(cmd.Context(), args[0], lsn)
		},
	}
	fromLSN string
)

func init() {
	Cmd.AddCommand(catchupPushCmd)

	catchupPushCmd.Flags().StringVar(&fromLSN, "from-lsn", "0",
		"LSN to start incremental backup, in X/X format (e.g. 0/EDA81980) or as a decimal number")
}
