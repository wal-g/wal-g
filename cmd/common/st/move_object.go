package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const moveObjectShortDescription = "" // TODO

// removeCmd represents the deleteObject command
var moveObjectCmd = &cobra.Command{
	Use:   "mv source_path destination_path",
	Short: moveObjectShortDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		srcPath := args[0]
		dstPath := args[1]

		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			return storagetools.HandleMoveObject(srcPath, dstPath, folder)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	StorageToolsCmd.AddCommand(moveObjectCmd)
}
