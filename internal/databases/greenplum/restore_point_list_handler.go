package greenplum

import (
	"errors"
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// TODO: unit tests
func HandleRestorePointList(folder storage.Folder, metaFetcher internal.GenericMetaFetcher, pretty, json bool) {
	restorePoints, err := GetRestorePoints(folder)
	if _, ok := err.(NoRestorePointsFoundError); ok {
		err = nil
	}
	tracelog.ErrorLogger.FatalfOnError("Get restore points from folder: %v", err)

	var errs []error
	// TODO: remove this ugly hack to make current restore-point-list work
	backupTimesWithMeta := make([]internal.BackupTimeWithMetadata, 0, len(restorePoints))
	for _, rpt := range restorePoints {
		metadata, err := metaFetcher.Fetch(rpt.Name, folder)
		if err != nil {
			errs = append(errs, err)
		}

		backupTimesWithMeta = append(backupTimesWithMeta, internal.BackupTimeWithMetadata{
			BackupTime: internal.BackupTime{
				BackupName:  rpt.Name,
				Time:        rpt.Time,
				WalFileName: utility.StripWalFileName(rpt.Name),
			},
			GenericMetadata: metadata,
		})
	}
	tracelog.ErrorLogger.FatalfOnError("Fetch metadata for restore points: %v", errors.Join(errs...))

	internal.SortBackupTimeWithMetadataSlices(backupTimesWithMeta)

	printableEntities := make([]printlist.Entity, len(backupTimesWithMeta))
	for i := range backupTimesWithMeta {
		printableEntities[i] = backupTimesWithMeta[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print restore points: %v", err)
}
