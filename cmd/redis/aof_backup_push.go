package redis

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	client "github.com/wal-g/wal-g/internal/databases/redis/client"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/utility"
)

var (
	sharded = false
)

const (
	aofBackupPushCommandName = "aof-backup-push"

	shardedShortDescription = "Turns on collecting slots info (use for sharded restore of sharded cluster only)"
	shardedFlag             = "sharded"
	shardedShorthand        = "s"
)

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
		logging.FatalOnError(err)

		uploader.ChangeDirectory(utility.BaseBackupPath + "/")

		memoryDataGetter := client.NewServerDataGetter()

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

		pushArgs := redis.AOFBackupPushArgs{
			Uploader:        uploader,
			MetaConstructor: metaConstructor,
			Sharded:         sharded,
		}

		err = redis.HandleAOFBackupPush(ctx, pushArgs)
		logging.FatalOnError(err)
	},
}

func init() {
	aofBackupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	aofBackupPushCmd.Flags().BoolVarP(&sharded, shardedFlag, shardedShorthand, false, "Pushes sharded backup")
	cmd.AddCommand(aofBackupPushCmd)
}
