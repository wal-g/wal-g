package mongo

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
)

func HandleBinaryFetchPush(ctx context.Context, mongodConfigPath, backupName, clusterMongodVersion string) error {
	config, err := binary.CreateMongodConfig(mongodConfigPath)
	if err != nil {
		return err
	}

	localStorage := binary.CreateLocalStorage(config.GetDBPath())

	backupStorage, err := binary.CreateBackupStorage(backupName)
	if err != nil {
		return err
	}

	restoreService, err := binary.CreateRestoreService(ctx, localStorage, backupStorage, config)
	if err != nil {
		return err
	}

	return restoreService.DoRestore(clusterMongodVersion)
}
