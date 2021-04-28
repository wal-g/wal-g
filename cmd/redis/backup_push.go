package redis

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/utility"
)

var (
	permanent = false
)

const (
	backupPushShortDescription = "Makes backup and uploads it to storage"
	PermanentFlag              = "permanent"
	PermanentShorthand         = "p"
)

// backupPushCmd represents the backupPush command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		// Configure folder
		uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

		backupCmd, err := internal.GetCommandSettingContext(ctx, internal.NameStreamCreateCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		backupCmd.Stderr = os.Stderr
		metaProvider := archive.NewBackupMetaRedisProvider(ctx, uploader.UploadingFolder, permanent)

		err = redis.HandleBackupPush(uploader, backupCmd, metaProvider)
		tracelog.ErrorLogger.FatalfOnError("Backup creation failed: %v", err)
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		internal.RequiredSettings[internal.NameStreamCreateCmd] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	backupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes backup with 'permanent' flag")
	cmd.AddCommand(backupPushCmd)
}

