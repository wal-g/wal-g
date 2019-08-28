package mysql

import (
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"strings"

	"github.com/spf13/cobra"
)

const StreamPushShortDescription = ""

// streamPushCmd represents the streamPush command
var streamPushCmd = &cobra.Command{
	Use:   "stream-push command\\ [command\\ args]",
	Short: StreamPushShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		command := strings.Split(args[0], " ")
		mysql.HandleStreamPush(&mysql.Uploader{Uploader: uploader}, command)
	},
}

func init() {
	Cmd.AddCommand(streamPushCmd)
}
