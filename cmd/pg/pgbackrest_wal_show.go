package pg

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/databases/postgres/pgbackrest"
)

var pgbackrestWalgShowCmd = &cobra.Command{
	Use:   "wal-show",
	Short: WalShowUsage,
	Long:  WalShowLongDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, stanza := configurePgbackrestSettings()
		outputType := postgres.TableOutput
		if detailedJSONOutput {
			outputType = postgres.JSONOutput
		}
		outputWriter := postgres.NewWalShowOutputWriter(outputType, os.Stdout, false)
		err := pgbackrest.HandleWalShow(folder, stanza, outputWriter)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	pgbackrestCmd.AddCommand(pgbackrestWalgShowCmd)
	pgbackrestWalgShowCmd.Flags().BoolVar(&detailedJSONOutput, detailedOutputFlag, false, detailedOutputDescription)
}
