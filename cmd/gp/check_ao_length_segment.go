package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
)

var (
	port   string
	segnum string
)

var checkAOLengthSegmentCmd = &cobra.Command{
	Use:   "check-ao-aocs-length-segment",
	Short: "Checks ao and aocs tables` EOF on disk is no less than in metadata for current segment",
	Run: func(cmd *cobra.Command, args []string) {
		handler, err := greenplum.NewAOLengthCheckSegmentHandler(port, segnum)
		tracelog.ErrorLogger.FatalOnError(err)
		handler.CheckAOTableLengthSegment()
	},
}

func init() {
	checkAOLengthSegmentCmd.PersistentFlags().StringVarP(&port, "port", "p", "5432", `database port (default: "5432")`)
	checkAOLengthSegmentCmd.PersistentFlags().StringVarP(&segnum, "segnum", "s", "", `database segment number`)

	cmd.AddCommand(checkAOLengthSegmentCmd)
}
