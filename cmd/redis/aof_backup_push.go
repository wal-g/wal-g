package redis

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	client "github.com/wal-g/wal-g/internal/databases/redis/client"
	"github.com/wal-g/wal-g/utility"
)

const aofBackupPushCommandName = "aof-backup-push"

var aofBackupPushCmd = &cobra.Command{
	Use:   aofBackupPushCommandName,
	Short: "Creates redis aof backup and pushes it to storage without local disk",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		uploader.ChangeDirectory(utility.BaseBackupPath + "/")

		memoryDataGetter := client.NewMemoryDataGetter()

		processName, _ := conf.GetSetting(conf.RedisServerProcessName)
		versionParser := archive.NewVersionParser(processName)

		metaConstructor := archive.NewBackupRedisMetaConstructor(
			ctx,
			uploader.Folder(),
			permanent,
			archive.AOFBackupType,
			versionParser,
			memoryDataGetter,
		)

		err = redis.HandleAOFBackupPush(ctx, permanent, uploader, metaConstructor)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	aofBackupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	cmd.AddCommand(aofBackupPushCmd)
}
