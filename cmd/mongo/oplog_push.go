package mongo

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/tracelog"

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
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		mongo.HandleOplogPush(&mongo.Uploader{Uploader: uploader})
	},
}

func init() {
	MongoCmd.AddCommand(oplogPushCmd)
}
