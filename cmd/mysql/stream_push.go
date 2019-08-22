package mysql

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/tinsane/tracelog"

	"github.com/spf13/cobra"
)

const StreamPushShortDescription = ""

// streamPushCmd represents the streamPush command
var streamPushCmd = &cobra.Command{
	Use:   "stream-push",
	Short: StreamPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleStreamPush(&mysql.Uploader{Uploader: uploader})
	},
}

func init() {
	Cmd.AddCommand(streamPushCmd)
}
