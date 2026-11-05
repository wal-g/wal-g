package redis

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupDelete(ctx context.Context, folder storage.Folder, backupName string, dryRun bool) error {
	// processing possible LATEST backupName, that's why existence check
	backup, err := archive.SentinelWithExistenceCheck(ctx, folder, backupName)
	if err != nil {
		return err
	}
	if backup.Permanent {
		return fmt.Errorf("cannot delete permanent backup %s", backup.Name())
	}

	if dryRun {
		tracelog.InfoLogger.Printf("Skipping backup deletion due to dry-run: %+v", backup)
		return nil
	}

	internalFolder := backup.ToInternal(folder).Folder
	if err := internal.DeleteBackups(ctx, internalFolder, []string{backup.Name()}); err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Backup was deleted: %+v", backup)
	return nil
}
