package internal

import (
	"os"

	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleDefaultBackupList(folder storage.Folder, pretty, json bool) {
	backupTimes, err := GetBackups(folder)
	err = FilterOutNoBackupFoundError(err, json)
	logging.FatalfOnError("Get backups from folder: %v", err)

	SortBackupTimeSlices(backupTimes)

	printableEntities := make([]printlist.Entity, len(backupTimes))
	for i := range backupTimes {
		printableEntities[i] = backupTimes[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	logging.FatalfOnError("Print backups: %v", err)
}
