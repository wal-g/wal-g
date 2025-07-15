package gp

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

var (
	logsDir        string
	runBackupCheck bool
	name           string
)

var checkAOTableLengthMasterCmd = &cobra.Command{
	Use:   "check-ao-aocs-length",
	Short: "Runs on master and checks ao and aocs tables` EOF on disk is no less than in metadata for all segments",
	Run: func(cmd *cobra.Command, args []string) {
		rootFolder, err := getMultistorageRootFolder(false, policies.UniteAllStorages)
		tracelog.ErrorLogger.FatalOnError(err)
		handler, err := greenplum.NewAOLengthCheckHandler(logsDir, runBackupCheck, name, rootFolder)
		tracelog.ErrorLogger.FatalOnError(err)
		handler.CheckAOTableLength()
	},
}

func init() {
	checkAOTableLengthMasterCmd.PersistentFlags().StringVarP(&logsDir, "logs", "l", viper.GetString(conf.GPLogsDirectory),
		"directory to store logs")
	checkAOTableLengthMasterCmd.PersistentFlags().BoolVar(&runBackupCheck, "check-backup", false,
		"if the flag is set, checks backup`s length")
	checkAOTableLengthMasterCmd.PersistentFlags().StringVarP(&name, "backup-name", "n", internal.LatestString,
		"sets name of backup to check, checks last when empty")

	cmd.AddCommand(checkAOTableLengthMasterCmd)
}
