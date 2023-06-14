package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	putObjectShortDescription = "Upload the specified file to the storage"

	noEncryptFlag      = "no-encrypt"
	noCompressFlag     = "no-compress"
	overwriteFlag      = "force"
	overwriteShorthand = "f"
)

// putObjectCmd represents the putObject command
var putObjectCmd = &cobra.Command{
	Use:   "put local_path destination_path",
	Short: putObjectShortDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		localPath := args[0]
		dstPath := args[1]

		err = multistorage.ExecuteOnStorage(targetStorage, func(folder storage.Folder) error {
			return storagetools.HandlePutObject(localPath, dstPath, uploader, overwrite, !noEncrypt, !noCompress)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

var noEncrypt bool
var noCompress bool
var overwrite bool

func init() {
	StorageToolsCmd.AddCommand(putObjectCmd)
	putObjectCmd.Flags().BoolVar(&noEncrypt, noEncryptFlag, false, "Do not encrypt the object")
	putObjectCmd.Flags().BoolVar(&noCompress, noCompressFlag, false, "Do not compress the object")
	putObjectCmd.Flags().BoolVarP(&overwrite, overwriteFlag, overwriteShorthand,
		false, "Overwrite the existing object")
}
