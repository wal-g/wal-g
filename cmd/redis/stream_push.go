package redis

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

const streamPushShortDescription = "Makes backup and uploads it to storage"

// streamPushCmd represents the streamPush command
var streamPushCmd = &cobra.Command{
	Use:   "stream-push",
	Short: streamPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		redis.HandleStreamPush(&redis.Uploader{Uploader: uploader})
	},
}

func init() {
	Cmd.AddCommand(streamPushCmd)
}
