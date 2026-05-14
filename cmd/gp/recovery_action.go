package gp

import (
	"github.com/spf13/viper"

	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum"

	"github.com/spf13/cobra"
)

const (
	recoveryActionDescription = "Update recovery.conf recovery_target_action"
)

var (
	actionCmd = &cobra.Command{
		Use:   "recovery-action [promote|pause|shutdown]",
		Short: recoveryActionDescription,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			logsDir := viper.GetString(conf.GPLogsDirectory)
			follower := greenplum.NewActionHandler(logsDir, restoreConfigPath, withMirrors)
			follower.UpdateAction(args[0])
		},
	}
)

func init() {
	actionCmd.Flags().StringVar(&restoreConfigPath, "restore-config", "", restoreConfigPathDescription)
	_ = actionCmd.MarkFlagRequired("restore-config")
	actionCmd.Flags().BoolVar(&withMirrors, "with-mirrors", false, withMirrorsFlagDescription)
	cmd.AddCommand(actionCmd)
}
