package gp

import (
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/cobra"
)

const (
	followPrimaryDescription = "Resumes cluster recovery using primary-created restore points " +
		"to apply transaction logs and advance recovery state"
)

var (
	// followPrimaryCmd represents the followPrimaryCmd command
	followPrimaryCmd = &cobra.Command{
		Use:   "follow-primary restore_point",
		Short: followPrimaryDescription,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			logsDir := viper.GetString(conf.GPLogsDirectory)
			rootFolder, err := getMultistorageRootFolder(true, policies.UniteAllStorages)
			tracelog.ErrorLogger.FatalOnError(err)
			follower := greenplum.NewFollowPrimaryHandler(rootFolder.GetSubFolder(utility.BaseBackupPath), logsDir, restoreConfigPath, args[0], timeout)
			follower.Follow()
		},
	}
	timeout int
)

func init() {
	followPrimaryCmd.Flags().StringVar(&restoreConfigPath, "restore-config", "", restoreConfigPathDescription)
	followPrimaryCmd.Flags().IntVarP(&timeout, "timeout", "t", 60000, "timeout (in seconds)")
	_ = followPrimaryCmd.MarkFlagRequired("restore-config")
	cmd.AddCommand(followPrimaryCmd)
}
