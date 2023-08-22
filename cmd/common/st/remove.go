package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const removeShortDescription = "Removes objects by the prefix from the specified storage"

// removeCmd represents the deleteObject command
var removeCmd = &cobra.Command{
	Use:   "rm prefix",
	Short: removeShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			return storagetools.HandleRemove(args[0], folder)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	StorageToolsCmd.AddCommand(removeCmd)
}
