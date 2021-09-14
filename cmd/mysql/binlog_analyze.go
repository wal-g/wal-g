package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	binlogListShortDescription = "Prints infor for available binlogs"
	ShallowFlag                = "shallow"
)

var (
	binlogListCmd = &cobra.Command{
		Use:   "binlog-analyze",
		Short: binlogListShortDescription,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			mysql.HandleBinlogAnalyze(shallow, pretty, json)
		},
	}
	shallow = false
)

func init() {
	cmd.AddCommand(binlogListCmd)

	binlogListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	binlogListCmd.Flags().BoolVar(&shallow, ShallowFlag, false, "Don't parse whole file - peek first GTID")
	binlogListCmd.Flags().BoolVar(&json, JSONFlag, false, "Prints output in json format")
}
