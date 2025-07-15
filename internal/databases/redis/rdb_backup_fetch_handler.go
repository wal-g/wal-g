package redis

import (
	"context"
	"os/exec"
	"path/filepath"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd, skipClean bool) error {
	backup, err := archive.SentinelWithExistenceCheck(folder, backupName)
	if err != nil {
		return err
	}

	if !skipClean {
		dataFolder, _ := conf.GetSetting(conf.RedisDataPath)
		aofFolder, _ := conf.GetSetting(conf.RedisAppendonlyFolder)
		aofPath := filepath.Join(dataFolder, aofFolder)
		aofFolderInfo := archive.CreateAofFolderInfo(aofPath)

		err = aofFolderInfo.CleanPathAndParent()
		if err != nil {
			return err
		}
	}

	return internal.StreamBackupToCommandStdin(restoreCmd, backup.ToInternal(folder))
}
