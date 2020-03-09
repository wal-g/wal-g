package mysql

import (
	"github.com/wal-g/tracelog"
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
		uploader, err := internal.ConfigureWalUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleBinlogPush(uploader)
	},
}

func init() {
	Cmd.AddCommand(binlogPushCmd)
}
