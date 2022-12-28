package mysql

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"

	"github.com/spf13/cobra"
)

const (
	binlogServerShortDescription = "Create server for backup slaves"
)

var (
	binlogServerCmd = &cobra.Command{
		Use:   "binlog-server",
		Short: binlogServerShortDescription,
		Args:  cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			internal.RequiredSettings[internal.MysqlBinlogServerHost] = true
			internal.RequiredSettings[internal.MysqlBinlogServerPort] = true
			internal.RequiredSettings[internal.MysqlBinlogServerUser] = true
			internal.RequiredSettings[internal.MysqlBinlogServerPassword] = true
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
		},
		Run: func(cmd *cobra.Command, args []string) {
			mysql.HandleBinlogServer()
		},
	}
)

func init() {
	cmd.AddCommand(binlogServerCmd)
}
