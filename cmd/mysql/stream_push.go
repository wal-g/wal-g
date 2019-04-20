package mysql

import (
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

const StreamPushShortDescription = ""

// streamPushCmd represents the streamPush command
var streamPushCmd = &cobra.Command{
	Use:   "stream-push",
	Short: StreamPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		mysql.HandleStreamPush(&mysql.Uploader{Uploader: uploader})
	},
}

func init()  {
	MySQLCmd.AddCommand(streamPushCmd)
}