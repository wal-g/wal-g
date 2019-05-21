package redis

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis" // TODO: replace with my repo
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

const backupPushShortDescription = ""

// backupPushCmd represents the cron command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		redis.HandleBackupPush(&redis.Uploader{Uploader: uploader})
	},
}

func init() {
	RedisCmd.AddCommand(backupPushCmd)
}
