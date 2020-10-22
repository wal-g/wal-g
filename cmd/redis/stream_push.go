package redis

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
)

const streamPushShortDescription = "Makes backup and uploads it to storage"

// streamPushCmd represents the streamPush command
var streamPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: streamPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)
		backupCmd, err := internal.GetCommandSetting(internal.NameStreamCreateCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		redis.HandleBackupPush(uploader, backupCmd)
	},
}

func init() {
	Cmd.AddCommand(streamPushCmd)
}
