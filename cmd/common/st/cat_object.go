package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storagetools"
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

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		storagetools.HandleCatObject(objectPath, folder, decrypt, decompress)
	},
}

var decrypt bool
var decompress bool

func init() {
	StorageToolsCmd.AddCommand(catObjectCmd)
	getObjectCmd.Flags().BoolVar(&decrypt, decryptFlag, false, "decrypt the object")
	getObjectCmd.Flags().BoolVar(&decompress, decompressFlag, false, "decompress the object")
}
