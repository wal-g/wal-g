package xb

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/databases/mysql/xbstream"
)

const (
	extractDiffXBStreamShortDescription = "(DANGEROUS) Apply diff from xbstream and put leftovers to incremental dir"
	incrementalDirFlag                  = "incremental-dir"
)

var (
	extractDiffXBStreamCmd = &cobra.Command{
		Use:   "extract-diff",
		Short: extractDiffXBStreamShortDescription,
		Args:  cobra.RangeArgs(0, 1),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var src *os.File
			if len(args) == 0 {
				src = os.Stdin
			} else {
				src, err = os.Open(args[0])
				tracelog.ErrorLogger.FatalfOnError("Cannot open input file: %v", err)
			}

			err = os.MkdirAll(dataDir, 0777) // FIXME: 0777? use UMASK?
			tracelog.ErrorLogger.FatalfOnError("Cannot create destination folder: %v", err)

			streamReader := xbstream.NewReader(src, false)
			xbstream.DiffBackupSink(streamReader, dataDir, incrementalDir)
		},
	}
	incrementalDir string
)

func init() {
	extractDiffXBStreamCmd.Flags().StringVar(&dataDir, dataDirFlag,
		"", "Directory where to extract base backup")
	extractDiffXBStreamCmd.Flags().StringVar(&incrementalDir, incrementalDirFlag,
		"", "Directory where to extract incremental backup")

	XBToolsCmd.AddCommand(extractDiffXBStreamCmd)
}
