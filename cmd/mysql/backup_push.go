package mysql

import (
	"github.com/spf13/cobra"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const StreamPushShortDescription = ""

// streamPushCmd represents the streamPush command
var streamPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: StreamPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader(true)
		tracelog.ErrorLogger.FatalOnError(err)
		command := internal.GetNameStreamCreateCmd()
		mysql.HandleBackupPush(&mysql.Uploader{Uploader: uploader}, command)
	},
}

func init() {
	Cmd.AddCommand(streamPushCmd)
}
