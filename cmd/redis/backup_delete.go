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

// purgeCmd represents the command for purging old backups (for deleting separate backup see deleteCmd)
var purgeCmd = &cobra.Command{
	Use:   "delete",
	Short: "Purge old backups",
	Run:   runPurge,
}

func runPurge(cmd *cobra.Command, args []string) {
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

	st, err := internal.ConfigureStorage()
	tracelog.ErrorLogger.FatalOnError(err)

	backupFolder := st.RootFolder().GetSubFolder(utility.BaseBackupPath)

	err = redis.HandlePurge(backupFolder, opts...)
	tracelog.ErrorLogger.FatalOnError(err)
}

func init() {
	cmd.AddCommand(purgeCmd)
	purgeCmd.Flags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup and garbage purge")
	purgeCmd.Flags().BoolVar(&purgeGarbage, purgeGarbageFlag, false, "Purge garbage in backup folder")
	purgeCmd.Flags().StringVar(&retainAfter, retainAfterFlag, "", "Keep backups newer")
	purgeCmd.Flags().UintVar(&retainCount, retainCountFlag, 0, "Keep minimum count, except permanent backups")
}
