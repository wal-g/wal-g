package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
)

var (
	logsDir        string
	runBackupCheck bool
)

var checkAOTableLengthMasterCmd = &cobra.Command{
	Use:   "check-ao-aocs-length",
	Short: "Runs on master and checks ao and aocs tables` EOF on disk is no less than in metadata for all segments",
	Run: func(cmd *cobra.Command, args []string) {
		handler, err := greenplum.NewAOLengthCheckHandler(logsDir, runBackupCheck)
		tracelog.ErrorLogger.FatalOnError(err)
		handler.CheckAOTableLength()
	},
}

func init() {
	checkAOTableLengthMasterCmd.PersistentFlags().StringVarP(&logsDir, "logs", "l", "/var/log/greenplum", `directory to store logs`)
	checkAOTableLengthMasterCmd.PersistentFlags().BoolVar(&runBackupCheck, "check-backup", false,
		"if the flag is set, checks last backup`s length")

	cmd.AddCommand(checkAOTableLengthMasterCmd)
}
