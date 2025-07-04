package mongo

import (
	"context"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/utility"
)

func HandlePartialBinaryFetch(
	ctx context.Context,
	mongodConfigPath, minimalConfigPath, backupName, restoreMongodVersion string,
	skipBackupDownload, skipReconfig, skipChecks bool,
	whitelist, blacklist []string,
) error {
	config, err := binary.CreateMongodConfig(mongodConfigPath)
	if err != nil {
		return err
	}

	localStorage := binary.CreateLocalStorage(config.GetDBPath())

	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath + "/")

	if minimalConfigPath == "" {
		minimalConfigPath, err = config.SaveConfigToTempFile("storage", "systemLog")
		if err != nil {
			return err
		}
	}

	restoreService, err := binary.CreateRestoreService(ctx, localStorage, uploader, minimalConfigPath)
	if err != nil {
		return err
	}

	backup, err := internal.GetBackupByName(backupName, "", uploader.Folder())
	if err != nil {
		return err
	}

	return restoreService.PartialRestore(
		whitelist, blacklist, binary.RestoreArgs{
			BackupName:         backup.Name,
			RestoreVersion:     restoreMongodVersion,
			SkipBackupDownload: skipBackupDownload,
			SkipChecks:         skipChecks,
			SkipMongoReconfig:  skipReconfig,
		},
	)
}
