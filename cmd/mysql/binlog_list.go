package mysql

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	binlogListShortDescription = "Prints available binlogs info"
	LocalFlag                  = "local"
	ShallowFlag                = "shallow"
)

var (
	binlogListCmd = &cobra.Command{
		Use:   "binlog-list",
		Short: binlogListShortDescription,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			mysql.HandleBinlogList(local, shallow, pretty, json)
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if !local {
				tracelog.ErrorLogger.FatalOnError(fmt.Errorf("remote show-bilnlog not implemented"))
			}
		},
	}
	local   = false
	shallow = false
)

func init() {
	cmd.AddCommand(binlogListCmd)

	binlogListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	binlogListCmd.Flags().BoolVar(&local, LocalFlag, false, "Show only local binlogs")
	binlogListCmd.Flags().BoolVar(&shallow, ShallowFlag, false, "Don't parse whole file - peek first GTID")
	binlogListCmd.Flags().BoolVar(&json, JSONFlag, false, "Prints output in json format")
}
