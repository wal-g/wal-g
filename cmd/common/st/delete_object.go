package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storagetools"
)

const deleteObjectShortDescription = "Delete the specified storage object"

// deleteObjectCmd represents the deleteObject command
var deleteObjectCmd = &cobra.Command{
	Use:   "rm relative_object_path",
	Short: deleteObjectShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		storagetools.HandleDeleteObject(args[0], folder)
	},
}

func init() {
	StorageToolsCmd.AddCommand(deleteObjectCmd)
}
