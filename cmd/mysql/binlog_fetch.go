package mysql

import (
	"github.com/spf13/cobra"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"time"
)

const binlogFetchShortDescription = ""
const sinceFlagShortDescription = ""
const untilFlagShortDescription = ""

var backupName string
var untilDt string

// binlogPushCmd represents the cron command
var binlogFetchCmd = &cobra.Command{
	Use:   "binlog-fetch",
	Short: binlogFetchShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		dt, err := time.Parse(time.RFC3339, untilDt)
		tracelog.ErrorLogger.FatalfOnError("Failed to parse until timestamp " +  untilDt, err, )
		tracelog.ErrorLogger.FatalOnError(mysql.HandleBinlogFetch(folder, backupName, dt))
	},
}

func init() {
	binlogFetchCmd.PersistentFlags().StringVar(&backupName, "since", "LATEST", sinceFlagShortDescription)
	binlogFetchCmd.PersistentFlags().StringVar(&untilDt, "until", time.Now().Format(time.RFC3339), untilFlagShortDescription)
	Cmd.AddCommand(binlogFetchCmd)
}
