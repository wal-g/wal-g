package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"time"
)

const binlogFetchShortDescription = "fetches binlog from storage"
const sinceFlagShortDescription = "backup name starting from which you want to take binlog"
const untilFlagShortDescription = "time in RFC3339 for PITR"
const applyFlagShortDescription = "Apply fetched binlogs"

var backupName string
var untilDt string
var apply bool

// binlogPushCmd represents the cron command
var binlogFetchCmd = &cobra.Command{
	Use:   "binlog-fetch",
	Short: binlogFetchShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalfOnError("Failed to parse until timestamp " +  untilDt, err)
		err = mysql.HandleBinlogFetch(folder, backupName, untilDt, apply)
		tracelog.ErrorLogger.FatalfOnError("binlog fetch failed", err)
	},
}

func init() {
	binlogFetchCmd.PersistentFlags().StringVar(&backupName, "since", "LATEST", sinceFlagShortDescription)
	binlogFetchCmd.PersistentFlags().StringVar(&untilDt, "until", time.Now().Format(time.RFC3339), untilFlagShortDescription)
	binlogFetchCmd.PersistentFlags().BoolVar(&apply, "apply", false, applyFlagShortDescription)
	Cmd.AddCommand(binlogFetchCmd)
}