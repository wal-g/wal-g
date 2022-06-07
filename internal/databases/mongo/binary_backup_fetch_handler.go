package mongo

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
)

func HandleBinaryFetchPush(ctx context.Context, dbPath, backupName, replSetName, mongodVersion string) error {
	localStorage := binary.CreateLocalStorage(dbPath)

	backupStorage, err := binary.CreateBackupStorage(backupName, replSetName)
	if err != nil {
		return err
	}

	restoreService, err := binary.CreateRestoreService(ctx, localStorage, backupStorage)
	if err != nil {
		return err
	}

	return restoreService.DoRestore(mongodVersion)
}
