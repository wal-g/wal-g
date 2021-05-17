package redis

import (
	"time"

	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	retainAfterFlag  = "retain-after"
	retainCountFlag  = "retain-count"
	purgeGarbageFlag = "purge-garbage"
)

var (
	confirmed    bool
	purgeGarbage bool
	retainAfter  string
	retainCount  uint
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete old backups",
	Run:   runDelete,
}

func runDelete(cmd *cobra.Command, args []string) {
	opts := []redis.PurgeOption{
		redis.PurgeDryRun(!confirmed),
		redis.PurgeGarbage(purgeGarbage),
	}

	if cmd.Flags().Changed(retainAfterFlag) {
		retainAfterTime, err := time.Parse(time.RFC3339, retainAfter)
		tracelog.ErrorLogger.FatalfOnError("Can not parse retain time: %v", err)
		opts = append(opts, redis.PurgeRetainAfter(retainAfterTime))
	}

	if cmd.Flags().Changed(retainCountFlag) {
		if retainCount == 0 {
			tracelog.ErrorLogger.Fatalln("Retain count can not be 0")
		}
		opts = append(opts, redis.PurgeRetainCount(int(retainCount)))
	}

	err := redis.HandlePurge(utility.BaseBackupPath, opts...)
	tracelog.ErrorLogger.FatalOnError(err)
}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteCmd.Flags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
	deleteCmd.Flags().BoolVar(&purgeGarbage, purgeGarbageFlag, false, "Delete garbage in backup folder")
	deleteCmd.Flags().StringVar(&retainAfter, retainAfterFlag, "", "Keep backups newer")
	deleteCmd.Flags().UintVar(&retainCount, retainCountFlag, 0, "Keep minimum count, except permanent backups")
}
