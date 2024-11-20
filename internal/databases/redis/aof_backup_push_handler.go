package redis

import (
	"context"
	"path/filepath"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis/aof"
	"github.com/wal-g/wal-g/internal/diskwatcher"

	"github.com/spf13/viper"
)

func HandleAOFBackupPush(ctx context.Context, permanent bool, uploader internal.Uploader, metaConstructor internal.MetaConstructor) error {
	backupName := aof.GenerateNewBackupName()

	dataFolder, _ := conf.GetSetting(conf.RedisDataPath)
	aofFolder, _ := conf.GetSetting(conf.RedisAppendonlyFolder)
	aofPath := filepath.Join(dataFolder, aofFolder)
	tmpPath, _ := conf.GetSetting(conf.RedisAppendonlyTmpFolder)
	concurrentUploader, err := internal.CreateConcurrentUploader(uploader, backupName, []string{aofPath, tmpPath}, true)
	if err != nil {
		return err
	}

	dataPath, _ := conf.GetSetting(conf.RedisDataPath)
	diskWatcher, err := diskwatcher.NewDiskWatcher(viper.GetInt(conf.RedisDataThreshold), dataPath, viper.GetInt(conf.RedisDataTimeout))
	if err != nil {
		return err
	}

	manifestName, _ := conf.GetSetting(conf.RedisAppendonlyManifest)
	backupFilesListProvider := aof.NewBackupFilesListProvider(aofPath, tmpPath, manifestName)

	filesPinner := aof.NewFilesPinner()

	backupService, err := aof.CreateBackupService(ctx, diskWatcher, concurrentUploader, metaConstructor, backupFilesListProvider, filesPinner)
	if err != nil {
		return err
	}

	return backupService.DoBackup(backupName, permanent)
}
