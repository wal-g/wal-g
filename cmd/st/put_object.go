package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storagetools"
)

const (
	putObjectShortDescription = "Upload the specified file to the storage"

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

		storagetools.HandlePutObject(localPath, dstPath, uploader, overwrite)
	},
}

var overwrite bool

func init() {
	StorageToolsCmd.AddCommand(putObjectCmd)
	putObjectCmd.Flags().BoolVarP(&overwrite, overwriteFlag, overwriteShorthand,
		false, "Overwrite the existing object")
}
