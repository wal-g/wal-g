package mongo

import (
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"

	"github.com/spf13/cobra"
)

const oplogPushShortDescription = ""

// oplogPushCmd represents the cron command
var oplogPushCmd = &cobra.Command{
	Use:   "oplog-push",
	Short: oplogPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		mongo.HandleOplogPush(&mongo.Uploader{Uploader: uploader})
	},
}

func init() {
	Cmd.AddCommand(oplogPushCmd)
}
