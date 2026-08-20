package redis

import (
	"context"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis/aof"
	"github.com/wal-g/wal-g/internal/diskwatcher"
)

type AOFBackupPushArgs struct {
	BackupName      string
	Sharded         bool
	Uploader        internal.Uploader
	MetaConstructor internal.MetaConstructor
	DeferSentinel   bool
}

// permanent bool, uploader internal.Uploader, metaConstructor internal.MetaConstructor
func HandleAOFBackupPush(ctx context.Context, args AOFBackupPushArgs) error {
	backupName := args.BackupName
	if backupName == "" {
		backupName = aof.GenerateNewBackupName()
	}

	dataFolder, _ := conf.GetSetting(conf.RedisDataPath)
	aofFolder, _ := conf.GetSetting(conf.RedisAppendonlyFolder)
	aofPath := filepath.Join(dataFolder, aofFolder)
	tmpPath, _ := conf.GetSetting(conf.RedisAppendonlyTmpFolder)
	concurrentUploader, err := internal.CreateConcurrentUploader(
		ctx,
		internal.CreateConcurrentUploaderArgs{
			Uploader:   args.Uploader,
			BackupName: backupName,
			Directory:  tmpPath,
		})
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

	filesPinner := aof.NewFilesPinner(tmpPath)

	backupService, err := aof.CreateBackupService(
		diskWatcher,
		concurrentUploader,
		args.MetaConstructor,
		backupFilesListProvider,
		filesPinner,
	)
	if err != nil {
		return err
	}

	doBackupArgs := aof.DoBackupArgs{
		BackupName:    backupName,
		Sharded:       args.Sharded,
		DeferSentinel: args.DeferSentinel,
	}

	return backupService.DoBackup(ctx, doBackupArgs)
}
