package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storagetools"
)

const (
	getObjectShortDescription = "Download the specified storage object"

	noDecryptFlag    = "no-decrypt"
	noDecompressFlag = "no-decompress"
)

// getObjectCmd represents the getObject command
var getObjectCmd = &cobra.Command{
	Use:   "get relative_object_path destination_path",
	Short: getObjectShortDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		objectPath := args[0]
		dstPath := args[1]

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		storagetools.HandleGetObject(objectPath, dstPath, folder, !noDecrypt, !noDecompress)
	},
}

var noDecrypt bool
var noDecompress bool

func init() {
	StorageToolsCmd.AddCommand(getObjectCmd)
	getObjectCmd.Flags().BoolVar(&noDecrypt, noDecryptFlag, false, "Do not noDecrypt the object")
	getObjectCmd.Flags().BoolVar(&noDecompress, noDecompressFlag, false, "Do not noDecompress the object")
}
