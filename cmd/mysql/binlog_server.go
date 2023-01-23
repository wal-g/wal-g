package mysql

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/utility"
)

const (
	binlogServerShortDescription  = "Create server for backup slaves"
	giveEventsUntilFlagShortDescr = "time in RFC3339 for PITR"
)

var giveEventsUntilTS string

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
			mysql.HandleBinlogServer(giveEventsUntilTS)
		},
	}
)

func init() {
	binlogServerCmd.PersistentFlags().StringVar(&giveEventsUntilTS,
		"until",
		utility.TimeNowCrossPlatformUTC().Format(time.RFC3339),
		giveEventsUntilFlagShortDescr)
	cmd.AddCommand(binlogServerCmd)
}
