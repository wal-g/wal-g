package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalRestoreUsage            = "wal-restore target-pgdata source-pgdata"
	WalRestoreShortDescription = "Restores WAL segments from storage."
	WalRestoreLongDescription  = "Restores the missing WAL segments that will be needed to perform pg_rewind with storage."
)

// walRestoreCmd represents the walRestore command
var walRestoreCmd = &cobra.Command{
	Use:   WalRestoreUsage,
	Short: WalRestoreShortDescription,
	Long:  WalRestoreLongDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		cloudFolder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalfOnError("Error on configure external folder %v\n", err)

		internal.HandleWALRestore(args[0], args[1], cloudFolder)
	},
}

func init() {
	cmd.AddCommand(walRestoreCmd)
}
