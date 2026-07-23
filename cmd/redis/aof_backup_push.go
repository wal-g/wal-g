package redis

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	client "github.com/wal-g/wal-g/internal/databases/redis/client"
	"github.com/wal-g/wal-g/utility"
)

func runAOFBackupPush(ctx context.Context) error {
	uploader, err := internal.ConfigureUploader(ctx)
	if err != nil {
		return err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath + "/")

	processName, _ := conf.GetSetting(conf.RedisServerProcessName)
	versionParser := archive.NewVersionParser(processName)

	pushArgs := redisdb.AOFBackupPushArgs{
		Uploader: uploader,
		MetaConstructor: archive.NewBackupRedisMetaConstructor(
			uploader.Folder(), permanent, archive.AOFBackupType, versionParser, client.NewServerDataGetter(),
		),
		Sharded: sharded,
	}
	return redisdb.HandleAOFBackupPush(ctx, pushArgs)
}
