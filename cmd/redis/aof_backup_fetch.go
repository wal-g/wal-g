package redis

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/utility"
)

const (
	aofBackupFetchCommandName     = "aof-backup-fetch"
	SkipBackupDownloadFlag        = "skip-backup-download"
	SkipBackupDownloadDescription = "Skip backup download"
	SkipChecksFlag                = "skip-checks"
	SkipChecksDescription         = "Skip checking redis version on compatibility with backup"
)

var (
	skipBackupDownloadFlag bool
	skipCheckFlag          bool
)

var aofBackupFetchCmd = &cobra.Command{
	Use:   aofBackupFetchCommandName + " <backup name> <redis version>",
	Short: "Fetches a redis AOF backup from storage and restores it in redis storage",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		uploader, err := internal.ConfigureUploader(cmd.Context())
		tracelog.ErrorLogger.FatalOnError(err)

		sourceStorageFolder := uploader.Folder()
		uploader.ChangeDirectory(utility.BaseBackupPath + "/")

		backupName := args[0]
		redisVersion := args[1]

		err = redis.HandleAofFetchPush(cmd.Context(), sourceStorageFolder, uploader, backupName, redisVersion, skipBackupDownloadFlag,
			skipCheckFlag)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	aofBackupFetchCmd.Flags().BoolVar(&skipBackupDownloadFlag, SkipBackupDownloadFlag, false, SkipBackupDownloadDescription)
	aofBackupFetchCmd.Flags().BoolVar(&skipCheckFlag, SkipChecksFlag, false, SkipChecksDescription)
	cmd.AddCommand(aofBackupFetchCmd)
}
