package gp

import (
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"

	"github.com/spf13/cobra"
)

const (
	followPrimaryDescription = "Advances the restored cluster using cluster-wide restore points created by primary"
)

var (
	// followPrimaryCmd represents the followPrimaryCmd command
	followPrimaryCmd = &cobra.Command{
		Use:   "follow-primary restore_point",
		Short: followPrimaryDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			logsDir := viper.GetString(internal.GPLogsDirectory)

			follower := greenplum.NewFollowPrimaryHandler(logsDir, restoreConfigPath, args[0], timeout)
			follower.Follow()
		},
	}
	timeout int
)

func init() {
	followPrimaryCmd.Flags().StringVar(&restoreConfigPath, "restore-config",
		"", restoreConfigPathDescription)
	followPrimaryCmd.Flags().IntVarP(&timeout, "timeout", "t", 60000, "timeout (in seconds)")
	_ = followPrimaryCmd.MarkFlagRequired("restore-config")
	cmd.AddCommand(followPrimaryCmd)
}
