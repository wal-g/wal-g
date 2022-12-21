package greenplum

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func HandleRestorePointList(folder storage.Folder, pretty, json bool) {
	getRestorePointsFunc := func() ([]internal.BackupTime, error) {
		res, err := GetRestorePoints(folder)
		if _, ok := err.(NoRestorePointsFoundError); ok {
			err = nil
		}

		// TODO: remove this ugly hack to make current restore-point-list work
		backupTimes := make([]internal.BackupTime, 0)
		for _, rp := range res {
			backupTimes = append(backupTimes, internal.BackupTime{
				BackupName:  rp.Name,
				Time:        rp.Time,
				WalFileName: utility.StripWalFileName(rp.Name),
			})
		}
		return backupTimes, err
	}
	writeRestorePointsListFunc := func(restorePoints []internal.BackupTime) {
		internal.SortBackupTimeSlices(restorePoints)
		switch {
		case json:
			err := internal.WriteAsJSON(restorePoints, os.Stdout, pretty)
			tracelog.ErrorLogger.FatalOnError(err)
		case pretty:
			internal.WritePrettyBackupList(restorePoints, os.Stdout)
		default:
			internal.WriteBackupList(restorePoints, os.Stdout)
		}
	}
	logging := internal.Logging{
		InfoLogger:  tracelog.InfoLogger,
		ErrorLogger: tracelog.ErrorLogger,
	}

	internal.HandleBackupList(getRestorePointsFunc, writeRestorePointsListFunc, logging)
}
