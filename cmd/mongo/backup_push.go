package mongo

import (
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"

	"github.com/spf13/cobra"
)

const BackupPushShortDescription = ""

// backupPushCmd represents the backupPush command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: BackupPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		command := internal.GetNameStreamCreateCmd()
		mongo.HandleStreamPush(&mongo.Uploader{Uploader: uploader}, command)
	},
}

func init() {
	Cmd.AddCommand(backupPushCmd)
}
