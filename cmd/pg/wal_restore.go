package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/fs"
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
		targetFolder, err := fs.ConfigureFolder(args[0], nil)
		tracelog.ErrorLogger.FatalfOnError("Error on configure target folder %v\n", err)
		sourceFolder, err := fs.ConfigureFolder(args[1], nil)
		tracelog.ErrorLogger.FatalfOnError("Error on configure source folder %v\n", err)
		cloudFolder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalfOnError("Error on configure external folder %v\n", err)

		internal.HandleWALRestore(targetFolder, sourceFolder, cloudFolder)
	},
}

func init() {
	cmd.AddCommand(walRestoreCmd)
}
