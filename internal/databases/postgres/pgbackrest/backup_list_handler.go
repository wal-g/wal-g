package pgbackrest

import (
	"fmt"
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TODO: unit tests
func HandleBackupList(folder storage.Folder, metaFetcher internal.GenericMetaFetcher, stanza string, detailed bool, pretty bool, json bool) error {
	backupTimesWithMeta, err := GetBackupListWithMetadata(folder, metaFetcher, stanza)
	if err != nil {
		return err
	}

	if len(backupTimesWithMeta) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return nil
	}

	internal.SortBackupTimeWithMetadataSlices(backupTimesWithMeta)

	printableEntities := make([]printlist.Entity, len(backupTimesWithMeta))
	for i := range backupTimesWithMeta {
		if detailed {
			details, err := GetBackupDetails(folder, stanza, backupTimesWithMeta[i].BackupTime.BackupName)
			if err != nil {
				return fmt.Errorf("get backup details: %w", err)
			}
			printableEntities[i] = details
		} else {
			printableEntities[i] = backupTimesWithMeta[i]
		}
	}

	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	if err != nil {
		return fmt.Errorf("print backups: %w", err)
	}
	return nil
}
