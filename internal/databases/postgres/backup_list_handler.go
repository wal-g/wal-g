package postgres

import (
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleDetailedBackupList(folder storage.Folder, pretty bool, json bool) {
	backups, err := internal.GetBackups(folder)
	if errors.Is(err, internal.ErrNoBackupsFound) {
		// Having zero backups is not an error that should be handled.
		err = nil
	}
	tracelog.ErrorLogger.FatalfOnError("Get backups from folder: %v", err)

	backupDetails, err := GetBackupsDetails(folder, backups)
	tracelog.ErrorLogger.FatalOnError(err)

	SortBackupDetails(backupDetails)

	printableEntities := make([]printlist.Entity, len(backupDetails))
	for i := range backupDetails {
		printableEntities[i] = &backupDetails[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backups: %v", err)
}
