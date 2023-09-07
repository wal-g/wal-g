package greenplum

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// TODO: unit tests
func HandleRestorePointList(folder storage.Folder, pretty, json bool) {
	restorePoints, err := GetRestorePoints(folder)
	if _, ok := err.(NoRestorePointsFoundError); ok {
		err = nil
	}
	tracelog.ErrorLogger.FatalfOnError("Get restore points from folder: %v", err)

	// TODO: remove this ugly hack to make current restore-point-list work
	backupTimes := make([]internal.BackupTime, 0)
	for _, rpt := range restorePoints {
		backupTimes = append(backupTimes, internal.BackupTime{
			BackupName:  rpt.Name,
			Time:        rpt.Time,
			WalFileName: utility.StripWalFileName(rpt.Name),
		})
	}

	internal.SortBackupTimeSlices(backupTimes)

	printableEntities := make([]printlist.Entity, len(backupTimes))
	for i := range backupTimes {
		printableEntities[i] = backupTimes[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print restore points: %v", err)
}
