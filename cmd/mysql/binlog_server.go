package mysql

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/utility"
)

const (
	binlogServerShortDescription = "Create server for backup slaves"
	binlogSinceFlagShortDescr    = "backup name starting from which you want to use binlogs"
	untilFlagShortDescr          = "time in RFC3339 for PITR"
)

var untilTS string
var BinlogBackupName string

var (
	binlogServerCmd = &cobra.Command{
		Use:   "binlog-server",
		Short: binlogServerShortDescription,
		Args:  cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			conf.RequiredSettings[conf.MysqlBinlogServerHost] = true
			conf.RequiredSettings[conf.MysqlBinlogServerPort] = true
			conf.RequiredSettings[conf.MysqlBinlogServerUser] = true
			conf.RequiredSettings[conf.MysqlBinlogServerPassword] = true
			conf.RequiredSettings[conf.MysqlBinlogServerID] = true
			conf.RequiredSettings[conf.MysqlBinlogServerReplicaSource] = true
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
		},
		Run: func(cmd *cobra.Command, args []string) {
			mysql.HandleBinlogServer(BinlogBackupName, untilTS)
		},
	}
)

func init() {
	binlogServerCmd.Flags().StringVar(&BinlogBackupName, "since", "LATEST", binlogSinceFlagShortDescr)
	binlogServerCmd.Flags().StringVar(&untilTS,
		"until",
		utility.TimeNowCrossPlatformUTC().Format(time.RFC3339),
		untilFlagShortDescr)
	cmd.AddCommand(binlogServerCmd)
}
