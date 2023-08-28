package st

import (
	"io"
	"os"
	
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/internal/storagetools"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	putObjectShortDescription = "Upload the specified file to the storage"

	noEncryptFlag      = "no-encrypt"
	noCompressFlag     = "no-compress"
	overwriteFlag      = "force"
	overwriteShorthand = "f"
	readStdinFlag 	   = "read-stdin"
	readStdinShorthand = "s"
)


// putObjectCmd represents the putObject command
var putObjectCmd = &cobra.Command{
	Use:   "put local_path destination_path",
	Short: putObjectShortDescription,
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {

		if len(args) == 1 && !readStdin {
			tracelog.ErrorLogger.Fatal("should specify localPath on read-from stdin flag")
		}

		var dstPath string
		var reader io.Reader

		if !readStdin {
			localPath := args[0]
			dstPath = args[1]
			fileReadCloser, err := storagetools.OpenLocalFile(localPath)
			if err != nil {
				tracelog.ErrorLogger.FatalOnError(err)
			}

			reader = fileReadCloser

			defer fileReadCloser.Close()
		} else {
			dstPath = args[0]
			reader = os.Stdin
		}

		err := exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			uploader, err := internal.ConfigureUploaderToFolder(folder)
			if err != nil {
				return err
			}
			return storagetools.HandlePutObject(reader, dstPath, uploader, overwrite, !noEncrypt, !noCompress)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

var noEncrypt bool
var noCompress bool
var overwrite bool
var readStdin bool

func init() {
	StorageToolsCmd.AddCommand(putObjectCmd)
	putObjectCmd.Flags().BoolVar(&noEncrypt, noEncryptFlag, false, "Do not encrypt the object")
	putObjectCmd.Flags().BoolVar(&noCompress, noCompressFlag, false, "Do not compress the object")
	putObjectCmd.Flags().BoolVarP(&overwrite, overwriteFlag, overwriteShorthand,
		false, "Overwrite the existing object")
	putObjectCmd.Flags().BoolVarP(&readStdin, readStdinFlag, readStdinShorthand,
			false, "Read file content from STDIN")
}
