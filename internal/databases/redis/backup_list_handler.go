package redis

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TODO : unit tests
func HandleDetailedBackupList(folder storage.Folder, pretty bool, json bool) {
	backups, err := internal.GetBackups(folder)
	if len(backups) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return
	}
	tracelog.ErrorLogger.FatalOnError(err)

	backupDetails, err := GetBackupsDetails(folder, backups)
	tracelog.ErrorLogger.FatalOnError(err)

	printableEntities := make([]printlist.Entity, len(backupDetails))
	for i := range backupDetails {
		printableEntities[i] = backupDetails[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backups: %v", err)
}

func GetBackupsDetails(folder storage.Folder, backups []internal.BackupTime) ([]archive.Backup, error) {
	backupsDetails := make([]archive.Backup, 0, len(backups))
	for i := len(backups) - 1; i >= 0; i-- {
		details, err := GetBackupDetails(folder, backups[i])
		if err != nil {
			return nil, err
		}
		backupsDetails = append(backupsDetails, details)
	}
	return backupsDetails, nil
}

func GetBackupDetails(folder storage.Folder, backupTime internal.BackupTime) (archive.Backup, error) {
	backup, err := internal.NewBackup(folder, backupTime.BackupName)
	if err != nil {
		return archive.Backup{}, err
	}

	metaData := archive.Backup{}
	err = backup.FetchSentinel(&metaData)
	if err != nil {
		return archive.Backup{}, err
	}
	return metaData, nil
}
