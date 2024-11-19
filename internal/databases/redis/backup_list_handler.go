package redis

import (
	"os"
	"sort"

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

	backupDetails, err := GetBackupDetails(folder, backups)
	tracelog.ErrorLogger.FatalOnError(err)

	printableEntities := make([]printlist.Entity, len(backupDetails))
	for i := range backupDetails {
		printableEntities[i] = backupDetails[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backups: %v", err)
}

func GetBackupDetails(folder storage.Folder, backups []internal.BackupTime) ([]archive.Backup, error) {
	backupDetails := make([]archive.Backup, 0, len(backups))
	for i := len(backups) - 1; i >= 0; i-- {
		details, err := archive.SentinelWithoutExistenceCheck(folder, backups[i].BackupName)
		if err != nil {
			return nil, err
		}
		backupDetails = append(backupDetails, details)
	}

	sort.Slice(backupDetails, func(i, j int) bool {
		return backupDetails[i].FinishLocalTime.Before(backupDetails[j].FinishLocalTime)
	})

	return backupDetails, nil
}
