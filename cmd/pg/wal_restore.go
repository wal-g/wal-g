package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	WalRestoreUsage            = "wal-restore"
	WalRestoreShortDescription = "Restores WAL segments from storage."
	WalRestoreLongDescription  = "Restores the missing WAL segments that will be needed to perform pg_rewind with storage."
)

// walRestoreCmd represents the walRestore command
var walRestoreCmd = &cobra.Command{
	Use:   WalRestoreUsage,
	Short: WalRestoreShortDescription,
	Long:  WalRestoreLongDescription,
	Run: func(cmd *cobra.Command, checks []string) {
		walDirectory := internal.GetWalFolderPath()
		externalFolder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		internal.HandleWALRestore(externalFolder, walDirectory)
	},
}

func init() {
	cmd.AddCommand(walRestoreCmd)
}
