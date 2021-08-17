package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storagetools"
)

const folderListShortDescription = "Prints objects in the provided storage folder"

// folderListCmd represents the folderList command
var folderListCmd = &cobra.Command{
	Use:   "ls [relative folder path]",
	Short: folderListShortDescription,
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		if len(args) > 0 {
			folder = folder.GetSubFolder(args[0])
		}

		storagetools.HandleFolderList(folder)
	},
}

func init() {
	StorageToolsCmd.AddCommand(folderListCmd)
}
