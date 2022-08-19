package mongo

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
)

func HandleBinaryBackupPush(ctx context.Context, permanent bool, appName string) error {
	backupName := binary.GenerateNewBackupName()

	mongodbURI, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
	if err != nil {
		return err
	}
	mongodService, err := binary.CreateMongodService(ctx, appName, mongodbURI)
	if err != nil {
		return err
	}

	mongodConfig, err := mongodService.MongodConfig()
	if err != nil {
		return err
	}

	localStorage := binary.CreateLocalStorage(mongodConfig.Storage.DBPath)

	backupStorage, err := binary.CreateBackupStorage(backupName)
	if err != nil {
		return err
	}

	backupService, err := binary.CreateBackupService(ctx, mongodService, localStorage, backupStorage)
	if err != nil {
		return err
	}

	return backupService.DoBackup(backupName, permanent)
}
