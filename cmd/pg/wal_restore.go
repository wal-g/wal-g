package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	WalRestoreUsage            = "wal-restore target-pgdata source-pgdata"
	WalRestoreShortDescription = "Restores WAL segments from storage."
	WalRestoreLongDescription  = "Restores the missing WAL segments that will be needed to perform pg_rewind from storage."
)

// walRestoreCmd represents the walRestore command
var walRestoreCmd = &cobra.Command{
	Use:   WalRestoreUsage,
	Short: WalRestoreShortDescription,
	Long:  WalRestoreLongDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalfOnError("Error on configure external folder %v\n", err)
		postgres.HandleWALRestore(args[0], args[1], folder)
	},
}

func init() {
	Cmd.AddCommand(walRestoreCmd)
}
