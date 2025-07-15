package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

var (
	port        string
	segnum      string
	checkBackup bool
	backupName  string
)

var checkAOLengthSegmentCmd = &cobra.Command{
	Use:   "check-ao-aocs-length-segment",
	Short: "Checks ao and aocs tables` EOF on disk is no less than in metadata for current segment",
	Run: func(cmd *cobra.Command, args []string) {
		rootFolder, err := getMultistorageRootFolder(false, policies.UniteAllStorages)
		tracelog.ErrorLogger.FatalOnError(err)
		handler, err := greenplum.NewAOLengthCheckSegmentHandler(port, segnum, rootFolder)
		tracelog.ErrorLogger.FatalOnError(err)
		if checkBackup {
			handler.CheckAOBackupLengthSegment(backupName)
		} else {
			handler.CheckAOTableLengthSegment()
		}
	},
}

func init() {
	checkAOLengthSegmentCmd.PersistentFlags().StringVarP(&port, "port", "p", "5432", `database port (default: "5432")`)
	checkAOLengthSegmentCmd.PersistentFlags().StringVarP(&segnum, "segnum", "s", "", `database segment number`)
	checkAOLengthSegmentCmd.PersistentFlags().BoolVar(&checkBackup, "check-backup", false,
		"if the flag is set, checks backup`s length")
	checkAOLengthSegmentCmd.PersistentFlags().StringVarP(&backupName, "backup-name", "n", internal.LatestString,
		"sets name of backup to check, checks last when empty")

	cmd.AddCommand(checkAOLengthSegmentCmd)
}
