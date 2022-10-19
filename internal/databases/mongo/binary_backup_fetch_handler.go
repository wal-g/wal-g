package mongo

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
)

func HandleBinaryFetchPush(ctx context.Context, mongodConfigPath, minimalConfigPath, backupName,
	restoreMongodVersion string,
) error {
	config, err := binary.CreateMongodConfig(mongodConfigPath)
	if err != nil {
		return err
	}

	localStorage := binary.CreateLocalStorage(config.GetDBPath())

	backupStorage, err := binary.CreateBackupStorage(backupName)
	if err != nil {
		return err
	}

	if minimalConfigPath == "" {
		minimalConfigPath, err = config.SaveConfigToTempFile("storage", "systemLog")
		if err != nil {
			return err
		}
	}

	restoreService, err := binary.CreateRestoreService(ctx, localStorage, backupStorage, minimalConfigPath)
	if err != nil {
		return err
	}

	return restoreService.DoRestore(restoreMongodVersion)
}
