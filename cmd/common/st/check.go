package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storagetools"
)

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "check access to the storage",
}

var checkReadCmd = &cobra.Command{
	Use:   "read [filename1 filename2 ...]",
	Short: "check read access to the storage",
	Args:  cobra.MinimumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		storagetools.HandleCheckRead(folder, args)
	},
}

var checkWriteCmd = &cobra.Command{
	Use:   "write",
	Short: "check write access to the storage",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		storagetools.HandleCheckWrite(folder)
	},
}

func init() {
	StorageToolsCmd.AddCommand(checkCmd)
	checkCmd.AddCommand(checkReadCmd)
	checkCmd.AddCommand(checkWriteCmd)
}
