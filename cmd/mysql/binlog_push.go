package mysql

import (
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"

	"github.com/spf13/cobra"
)

const binlogPushShortDescription = ""

// binlogPushCmd represents the cron command
var binlogPushCmd = &cobra.Command{
	Use:   "binlog-push",
	Short: binlogPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader(true)
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleBinlogPush(&mysql.Uploader{Uploader: uploader})
	},
}

func init() {
	Cmd.AddCommand(binlogPushCmd)
}
