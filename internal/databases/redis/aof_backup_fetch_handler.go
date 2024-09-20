package redis

import (
	"context"
	"path/filepath"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis/aof"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleAofFetchPush(
	ctx context.Context,
	sourceStorageFolder storage.Folder, uploader internal.Uploader,
	backupName, restoreVersion string,
	skipBackupDownload, skipChecks bool,
) error {
	dataFolder, _ := conf.GetSetting(conf.RedisDataPath)
	aofFolder, _ := conf.GetSetting(conf.RedisAppendonlyFolder)
	aofPath := filepath.Join(dataFolder, aofFolder)
	targetDiskFolder := archive.CreateAofFolderInfo(aofPath)

	processName, _ := conf.GetSetting(conf.RedisServerProcessName)
	versionParser := archive.NewVersionParser(processName)

	restoreService, err := aof.CreateRestoreService(ctx, sourceStorageFolder, targetDiskFolder, uploader, versionParser)
	if err != nil {
		return err
	}

	return restoreService.DoRestore(aof.RestoreArgs{
		BackupName:     backupName,
		RestoreVersion: restoreVersion,

		SkipChecks:         skipChecks,
		SkipBackupDownload: skipBackupDownload,
	})
}
