package internal

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleDefaultBackupList(folder storage.Folder, pretty, json bool) {
	backupTimes, err := GetBackups(folder)
	_, noBackupsErr := err.(NoBackupsFoundError)
	if noBackupsErr {
		tracelog.InfoLogger.Println("No backups found")
		return
	}
	tracelog.ErrorLogger.FatalfOnError("Get backups from folder: %v", err)

	SortBackupTimeSlices(backupTimes)

	printableEntities := make([]printlist.Entity, len(backupTimes))
	for i := range backupTimes {
		printableEntities[i] = backupTimes[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backups: %v", err)
}
