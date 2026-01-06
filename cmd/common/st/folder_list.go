package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const folderListShortDescription = "Prints objects in the provided storage folder"
const recursiveFlag = "recursive"
const recursiveShortHand = "r"
const allVersionsFlag = "all-versions"

// folderListCmd represents the folderList command
var folderListCmd = &cobra.Command{
	Use:   "ls [relative folder path]",
	Short: folderListShortDescription,
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		var path string
		if len(args) > 0 {
			path = args[0]
		} else {
			path = ""
		}

		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			if showAllVersions {
				storage.SetShowAllVersions(folder, true)
			}
			if glob {
				return storagetools.HandleFolderListWithGlob(folder, path, recursive)
			}
			subfolder := folder.GetSubFolder(path)
			return storagetools.HandleFolderList(subfolder, recursive)
		})
		if err != nil {
			tracelog.ErrorLogger.FatalOnError(err)
		}
	},
}

var recursive bool
var showAllVersions bool

func init() {
	folderListCmd.Flags().BoolVarP(&recursive, recursiveFlag, recursiveShortHand, false, "List folder recursively")
	folderListCmd.Flags().BoolVar(&showAllVersions, allVersionsFlag, false, "Show all object versions including deleted (S3 with versioning)")
	StorageToolsCmd.AddCommand(folderListCmd)
}
