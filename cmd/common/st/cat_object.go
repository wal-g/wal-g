package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	catObjectShortDescription = "Cat the specified storage object to STDOUT"

	decryptFlag    = "decrypt"
	decompressFlag = "decompress"
)

// catObjectCmd represents the catObject command
var catObjectCmd = &cobra.Command{
	Use:   "cat relative_object_path",
	Short: catObjectShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		objectPath := args[0]

		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			return storagetools.HandleCatObject(objectPath, folder, decrypt, decompress)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

var decrypt bool
var decompress bool

func init() {
	StorageToolsCmd.AddCommand(catObjectCmd)
	catObjectCmd.Flags().BoolVar(&decrypt, decryptFlag, false, "decrypt the object")
	catObjectCmd.Flags().BoolVar(&decompress, decompressFlag, false, "decompress the object")
}
