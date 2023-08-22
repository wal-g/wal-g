package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
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
		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			return storagetools.HandleCheckRead(folder, args)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

var checkWriteCmd = &cobra.Command{
	Use:   "write",
	Short: "check write access to the storage",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			return storagetools.HandleCheckWrite(folder)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	StorageToolsCmd.AddCommand(checkCmd)
	checkCmd.AddCommand(checkReadCmd)
	checkCmd.AddCommand(checkWriteCmd)
}
