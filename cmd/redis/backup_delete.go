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
	targetFlag       = "target"
	purgeGarbageFlag = "purge-garbage"
)

var (
	confirmed    bool
	purgeGarbage bool
	retainAfter  string
	retainCount  uint
	target       string
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

	if cmd.Flags().Changed(targetFlag) {
		if retainCount != 0 || retainAfter != "" {
			tracelog.ErrorLogger.Fatalln("Target and retain options cannot be used together")
		}
		opts = append(opts, redis.PurgeTarget(target))
	}

	err := redis.HandlePurge(utility.BaseBackupPath, opts...)
	tracelog.ErrorLogger.FatalOnError(err)
}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteCmd.Flags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup and garbage deletion")
	deleteCmd.Flags().BoolVar(&purgeGarbage, purgeGarbageFlag, false, "Delete garbage in backup folder")
	deleteCmd.Flags().StringVar(&retainAfter, retainAfterFlag, "", "Keep backups newer")
	deleteCmd.Flags().UintVar(&retainCount, retainCountFlag, 0, "Keep minimum count, except permanent backups")
	deleteCmd.Flags().StringVar(&target, targetFlag, "", "Delete backup by name")
}
