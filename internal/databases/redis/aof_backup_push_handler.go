package redis

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis/aof"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/diskwatcher"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/viper"
)

func HandleAOFBackupPush(ctx context.Context, permanent bool, appName string) error {
	backupName := aof.GenerateNewBackupName()

	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return err
	}

	uploader.ChangeDirectory(utility.BaseBackupPath + "/")
	aofPath, _ := conf.GetSetting(conf.RedisAppendonlyPath)
	concurrentUploader, err := internal.CreateConcurrentUploader(uploader, backupName, aofPath)
	if err != nil {
		return err
	}

	processName, _ := conf.GetSetting(conf.RedisServerProcessName)
	versionParser := archive.NewVersionParser(processName)

	metaConstructor := archive.NewBackupRedisMetaConstructor(ctx, uploader.Folder(), permanent, archive.AOFBackupType, versionParser)

	dataPath, _ := conf.GetSetting(conf.RedisDataPath)
	diskWatcher, err := diskwatcher.NewDiskWatcher(viper.GetInt(conf.RedisDataThreshold), dataPath, viper.GetInt(conf.RedisDataTimeout))
	if err != nil {
		return err
	}

	manifest, _ := conf.GetSetting(conf.RedisAppendonlyManifest)
	backupFilesListProvider := aof.NewBackupFilesListProvider(manifest)

	filesPinner := aof.NewFilesPinner()

	backupService, err := aof.CreateBackupService(ctx, diskWatcher, concurrentUploader, metaConstructor, backupFilesListProvider, filesPinner)
	if err != nil {
		return err
	}

	return backupService.DoBackup(backupName, permanent)
}
