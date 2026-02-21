package redis

import (
	"fmt"
	"log/slog"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupDelete(folder storage.Folder, backupName string, dryRun bool) error {
	// processing possible LATEST backupName, that's why existence check
	backup, err := archive.SentinelWithExistenceCheck(folder, backupName)
	if err != nil {
		slog.Info(fmt.Sprintf("Backup %s does not exist, nothing done: %+v", backupName, err))
		return nil
	}

	if dryRun {
		slog.Info(fmt.Sprintf("Skipping backup deletion due to dry-run: %+v", backup))
		return nil
	}

	internalFolder := backup.ToInternal(folder).Folder
	if err := internal.DeleteBackups(internalFolder, []string{backup.Name()}); err != nil {
		return err
	}
	slog.Info(fmt.Sprintf("Backup was deleted: %+v", backup))
	return nil
}
