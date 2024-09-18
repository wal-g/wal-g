package redis

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupDelete(folder storage.Folder, backupName string, dryRun bool) error {
	backup, err := BackupMeta(folder, backupName)
	if err != nil {
		return err
	}

	if dryRun {
		tracelog.InfoLogger.Printf("Skipping backup deletion due to dry-run: %+v", backup)
		return nil
	}

	if err := internal.DeleteBackups(folder, []string{backup.BackupName}); err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Backup was deleted: %+v", backup)
	return nil
}
