package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

var (
	confirmedWalPurge bool
)

// walPurgeCmd represents the wal purge command
var walPurgeCmd = &cobra.Command{
	Use:   "wal-purge",
	Short: "Purges outdated WAL archives from storage",
	Run:   runWalPurge,
}

func runWalPurge(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups, permanentWals := postgres.GetPermanentBackupsAndWals(folder)

	deleteHandler, err := postgres.NewDeleteHandler(folder, permanentBackups, permanentWals, true)
	tracelog.ErrorLogger.FatalOnError(err)

	err = postgres.HandleWalPurge(folder, deleteHandler, confirmedWalPurge)
	tracelog.ErrorLogger.FatalOnError(err)
}

func init() {
	Cmd.AddCommand(walPurgeCmd)
	walPurgeCmd.Flags().BoolVar(&confirmedWalPurge, internal.ConfirmFlag, false, "Confirms WAL archives deletion")
}
