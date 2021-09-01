package postgres

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// findOldestNonPermanentBackup finds oldest non-permanent backup available in storage.
func findOldestNonPermanentBackup(
	folder storage.Folder,
) (*BackupDetail, error) {
	backups, err := internal.GetBackups(folder)
	if err != nil {
		// this also includes the zero backups case
		return nil, err
	}

	backupDetails, err := GetBackupsDetails(folder, backups)
	if err != nil {
		return nil, err
	}

	SortBackupDetails(backupDetails)

	for i := range backupDetails {
		currBackup := &backupDetails[i]

		if currBackup.IsPermanent {
			tracelog.InfoLogger.Printf(
				"Backup %s is permanent, it is not eligible to be selected "+
					"as the oldest backup for wal-purge.\n", currBackup.BackupName)
			continue
		}
		tracelog.InfoLogger.Printf("Found earliest non-permanent backup: %s\n", currBackup.BackupName)
		return currBackup, nil
	}

	tracelog.WarningLogger.Printf("Could not find any non-permanent backups in storage.")
	return nil, internal.NewNoBackupsFoundError()
}

// HandleWalPurge delete outdated WAL archives
func HandleWalPurge(folder storage.Folder, deleteHandler *internal.DeleteHandler, confirm bool) error {
	oldestBackup, err := findOldestNonPermanentBackup(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return err
	}

	target, err := deleteHandler.FindTargetByName(oldestBackup.BackupName)
	if err != nil {
		return err
	}

	return deleteHandler.DeleteBeforeTargetWhere(target, confirm, func(object storage.Object) bool {
		objectName := object.GetName()
		// delete only WALs
		return len(objectName) >= len(utility.WalPath) && objectName[:len(utility.WalPath)] == utility.WalPath
	})
}
