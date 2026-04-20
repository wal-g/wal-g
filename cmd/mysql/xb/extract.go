package xb

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/wal-g/wal-g/internal/databases/mysql/xbstream"
	"github.com/wal-g/wal-g/internal/logging"
)

const (
	extractXBStreamShortDescription = "Extract xbstream to folder"
	decompressFlag                  = "decompress"
	decompressShorthand             = "d"
	dataDirFlag                     = "data-dir"
)

var (
	extractXBStreamCmd = &cobra.Command{
		Use:   "extract",
		Short: extractXBStreamShortDescription,
		Args:  cobra.RangeArgs(0, 1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var src *os.File
			if len(args) == 0 {
				src = os.Stdin
			} else {
				src, err = os.Open(args[0])
				logging.FatalfOnError("Cannot open input file: %v", err)
			}

			err = os.MkdirAll(dataDir, 0777) // FIXME: 0777? use UMASK?
			logging.FatalfOnError("Cannot create destination folder: %v", err)

			streamReader := xbstream.NewReader(src, false)
			xbstream.BackupSink(streamReader, dataDir, decompress)
		},
	}
	decompress bool
	dataDir    string
)

func init() {
	extractXBStreamCmd.Flags().BoolVarP(&decompress, decompressFlag, decompressShorthand,
		false, "Decompress files")
	extractXBStreamCmd.Flags().StringVar(&dataDir, dataDirFlag,
		"", "Directory where to extract files")

	XBToolsCmd.AddCommand(extractXBStreamCmd)
}
