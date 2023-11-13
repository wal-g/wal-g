package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
)

var (
	logsDir string
)

var checkAOTableLengthMasterCmd = &cobra.Command{
	Use:   "check-ao-aocs-length",
	Short: "Runs on master and checks ao and aocs tables` EOF on disk is no less than in metadata for all segments",
	Run: func(cmd *cobra.Command, args []string) {
		handler, err := greenplum.NewAOLengthCheckHandler(logsDir)
		tracelog.ErrorLogger.FatalOnError(err)
		handler.CheckAOTableLength()
	},
}

func init() {
	checkAOLengthSegmentCmd.PersistentFlags().StringVarP(&logsDir, "logs", "l", "/var/log/greenplum", `directory to store logs`)

	cmd.AddCommand(checkAOTableLengthMasterCmd)
}
