package mongo

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"

	"github.com/spf13/cobra"
)

const BackupPushShortDescription = "Pushes backup to storage"

// backupPushCmd represents the backupPush command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: BackupPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		command := internal.GetStreamCreateCmd()
		mongo.HandleStreamPush(&mongo.Uploader{Uploader: uploader}, command)
	},
}

func init() {
	Cmd.AddCommand(backupPushCmd)
}
