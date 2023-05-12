package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const folderListShortDescription = "Prints objects in the provided storage folder"
const recursiveFlag = "recursive"
const recursiveShortHand = "r"

// folderListCmd represents the folderList command
var folderListCmd = &cobra.Command{
	Use:   "ls [relative folder path]",
	Short: folderListShortDescription,
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		err := multistorage.ExecuteOnStorage(targetStorage, func(folder storage.Folder) error {
			if len(args) > 0 {
				folder = folder.GetSubFolder(args[0])
			}
			return storagetools.HandleFolderList(folder, recursive)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

var recursive bool

func init() {
	folderListCmd.Flags().BoolVarP(&recursive, recursiveFlag, recursiveShortHand, false, "List folder recursively")
	StorageToolsCmd.AddCommand(folderListCmd)
}
