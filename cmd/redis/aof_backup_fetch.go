package redis

import (
	"context"
	"os"
	"syscall"

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
	SkipChecksDescription         = "Skip checking valkey version on compatibility with backup"
)

var (
	skipBackupDownloadFlag bool
	skipCheckFlag          bool
)

var aofBackupFetchCmd = &cobra.Command{
	Use:   aofBackupFetchCommandName + " <backup name> <valkey version>",
	Short: "Fetches a valkey AOF backup from storage and restores it in valkey storage",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		sourceStorageFolder := uploader.Folder()
		uploader.ChangeDirectory(utility.BaseBackupPath + "/")

		backupName := args[0]
		valkeyVersion := args[1]

		err = redis.HandleAofFetchPush(ctx, sourceStorageFolder, uploader, backupName, valkeyVersion, skipBackupDownloadFlag, skipCheckFlag)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	aofBackupFetchCmd.Flags().BoolVar(&skipBackupDownloadFlag, SkipBackupDownloadFlag, false, SkipBackupDownloadDescription)
	aofBackupFetchCmd.Flags().BoolVar(&skipCheckFlag, SkipChecksFlag, false, SkipChecksDescription)
	cmd.AddCommand(aofBackupFetchCmd)
}
