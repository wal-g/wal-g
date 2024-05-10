package xb

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mysql/xbstream"
	"os"
)

const (
	extractXBStreamShortDescription = "Extract xbstream to folder"
)

var (
	extractXBStreamCmd = &cobra.Command{
		Use:   "extract",
		Short: extractXBStreamShortDescription,
		Args:  cobra.RangeArgs(1, 2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			var src *os.File
			var dst string
			if len(args) == 1 {
				src = os.Stdin
				dst = args[0]
			} else {
				src, err = os.Open(args[0])
				dst = args[1]
				tracelog.ErrorLogger.FatalfOnError("Cannot open input file: %v", err)
			}

			err = os.MkdirAll(dst, 0777)
			tracelog.ErrorLogger.FatalfOnError("Cannot create destination folder: %v", err)

			streamReader := xbstream.NewReader(src, false)
			xbstream.DiskSink(streamReader, dst)
		},
	}
)

func init() {
	XBToolsCmd.AddCommand(extractXBStreamCmd)
}
