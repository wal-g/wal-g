package mongo

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
)

func HandleBinaryBackupPush(ctx context.Context, permanent bool, appName string) error {
	backupName := binary.GenerateNewBackupName()

	mongodService, err := binary.CreateMongodService(ctx, appName)
	if err != nil {
		return err
	}

	mongodConfig, err := mongodService.MongodConfig()
	if err != nil {
		return err
	}

	replSetName, err := mongodService.GetReplSetName()
	if err != nil {
		return err
	}

	localStorage := binary.CreateLocalStorage(mongodConfig.Storage.DBPath)

	backupStorage, err := binary.CreateBackupStorage(backupName, replSetName)
	if err != nil {
		return err
	}

	backupService, err := binary.CreateBackupService(ctx, mongodService, localStorage, backupStorage)
	if err != nil {
		return err
	}

	return backupService.DoBackup(backupName, permanent)
}
