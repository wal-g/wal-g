package internal

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleDefaultBackupList(folder storage.Folder, metaFetcher GenericMetaFetcher, pretty, json bool) {
	backupTimesWithMeta, err := GetBackupsWithMetadata(folder, metaFetcher)
	_, noBackupsErr := err.(NoBackupsFoundError)
	if noBackupsErr {
		tracelog.InfoLogger.Println("No backups found")
		return
	}
	tracelog.ErrorLogger.FatalfOnError("Get backups from folder: %v", err)

	SortBackupTimeWithMetadataSlices(backupTimesWithMeta)

	printableEntities := make([]printlist.Entity, len(backupTimesWithMeta))
	for i := range backupTimesWithMeta {
		printableEntities[i] = backupTimesWithMeta[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backups: %v", err)
}
