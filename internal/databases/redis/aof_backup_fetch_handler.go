package redis

import (
	"context"
	"os"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis/aof"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/utility"
)

func HandleAofFetchPush(
	ctx context.Context,
	backupName, restoreMongodVersion string,
	skipBackupDownload, skipChecks bool,
) error {
	aofPath, _ := conf.GetSetting(conf.RedisAppendonlyPath)
	folder := archive.CreateFolder(aofPath, os.FileMode(0750))

	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath + "/")

	processName, _ := conf.GetSetting(conf.RedisServerProcessName)
	versionParser := archive.NewVersionParser(processName)

	restoreService, err := aof.CreateRestoreService(ctx, folder, uploader, versionParser)
	if err != nil {
		return err
	}

	backup, err := internal.GetBackupByName(backupName, "", uploader.Folder())
	if err != nil {
		return err
	}

	return restoreService.DoRestore(aof.RestoreArgs{
		BackupName:     backup.Name,
		RestoreVersion: restoreMongodVersion,

		SkipChecks:         skipChecks,
		SkipBackupDownload: skipBackupDownload,
	})
}
