package mysql

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/utility"
)

const fetchSinceFlagShortDescr = "backup name starting from which you want to fetch binlogs"
const fetchUntilFlagShortDescr = "time in RFC3339 for PITR"
const fetchUntilBinlogLastModifiedFlagShortDescr = "time in RFC3339 that is used to prevent wal-g from replaying" +
	" binlogs that was created/modified after this time"

var fetchBackupName string
var fetchUntilTS string
var fetchUntilBinlogLastModifiedTS string

// binlogPushCmd represents the cron command
var binlogFetchCmd = &cobra.Command{
	Use:   "binlog-fetch",
	Short: "fetches binlog from storage and save to the disk",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleBinlogFetch(folder, fetchBackupName, fetchUntilTS, fetchUntilBinlogLastModifiedTS)
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		internal.RequiredSettings[internal.MysqlBinlogDstSetting] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	binlogFetchCmd.PersistentFlags().StringVar(&fetchBackupName, "since", "LATEST", fetchSinceFlagShortDescr)
	binlogFetchCmd.PersistentFlags().StringVar(&fetchUntilTS,
		"until",
		utility.TimeNowCrossPlatformUTC().Format(time.RFC3339),
		fetchUntilFlagShortDescr)
	binlogFetchCmd.PersistentFlags().StringVar(&fetchUntilBinlogLastModifiedTS,
		"until-binlog-last-modified-time",
		"",
		fetchUntilBinlogLastModifiedFlagShortDescr)
	cmd.AddCommand(binlogFetchCmd)
}
