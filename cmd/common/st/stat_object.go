package st

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const statObjectShortDescription = "Prints metadata of the specified storage object"

// statObjectCmd represents the statObject command
var statObjectCmd = &cobra.Command{
	Use:   "stat relative_object_path",
	Short: statObjectShortDescription,
	Long: "Prints metadata of a single storage object in the same format as 'wal-g st ls'. " +
		"The metadata is fetched with a single cheap request (HEAD/stat) instead of listing the folder.",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		if glob {
			tracelog.ErrorLogger.FatalOnError(
				fmt.Errorf("the --glob flag isn't supported by 'stat', because expanding a pattern requires listing"))
		}
		err := exec.OnStorage(ctx, targetStorage, func(folder storage.Folder) error {
			return storagetools.HandleStatObject(ctx, args[0], folder)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	StorageToolsCmd.AddCommand(statObjectCmd)
}
