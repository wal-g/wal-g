package st

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storagetools"
)

const (
	getObjectShortDescription = "Download the specified storage object"

	downloadModeFlag      = "mode"
	downloadModeShorthand = "m"
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

		mode, err := ParseDownloadMode(downloadModeRaw)
		tracelog.ErrorLogger.FatalOnError(err)

		storagetools.HandleGetObject(objectPath, dstPath, folder, mode)
	},
}

var downloadModeRaw string

func ParseDownloadMode(mode string) (storagetools.DownloadMode, error) {
	switch mode {
	case "raw":
		return storagetools.DownloadRaw, nil
	case "decrypt":
		return storagetools.DownloadDecrypt, nil
	case "decompress":
		return storagetools.DownloadDecompress, nil
	default:
		return "", fmt.Errorf("unknown download mode: %s", mode)
	}
}

func init() {
	StorageToolsCmd.AddCommand(getObjectCmd)
	getObjectCmd.Flags().StringVarP(&downloadModeRaw, downloadModeFlag, downloadModeShorthand,
		"raw", "Download mode: raw/decrypt/decompress")
}
