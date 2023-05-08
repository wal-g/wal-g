package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const deleteObjectShortDescription = "Delete the specified storage object"

// deleteObjectCmd represents the deleteObject command
var deleteObjectCmd = &cobra.Command{
	Use:   "rm relative_object_path",
	Short: deleteObjectShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := multistorage.ExecuteOnStorage(targetStorage, func(folder storage.Folder) error {
			return storagetools.HandleDeleteObject(args[0], folder)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	StorageToolsCmd.AddCommand(deleteObjectCmd)
}
