package mysql

import (
	"github.com/spf13/cobra"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const StreamPushShortDescription = ""

// backupPushCmd represents the backupPush command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: StreamPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		command := internal.GetStreamCreateCmd()
		mysql.HandleBackupPush(&mysql.Uploader{Uploader: uploader}, command)
	},
}

func init() {
	Cmd.AddCommand(backupPushCmd)
}
