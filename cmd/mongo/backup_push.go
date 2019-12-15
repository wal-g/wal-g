package mongo

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"

	"github.com/spf13/cobra"
)

const StreamPushShortDescription = ""

// streamPushCmd represents the streamPush command
var streamPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: StreamPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		command := internal.GetNameStreamCreateCmd()
		mongo.HandleStreamPush(&mongo.Uploader{Uploader: uploader}, command)
	},
}

func init() {
	Cmd.AddCommand(streamPushCmd)
}
