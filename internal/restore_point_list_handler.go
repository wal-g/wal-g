package internal

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func DefaultHandleRestorePointList(folder storage.Folder, pretty, json bool) {
	getRestorePointsFunc := func() ([]BackupTime, error) {
		res, err := GetRestorePoints(folder)
		if _, ok := err.(NoBackupsFoundError); ok {
			err = nil
		}
		return res, err
	}
	writeRestorePointsListFunc := func(restorePoints []BackupTime) {
		SortBackupTimeSlices(restorePoints)
		switch {
		case json:
			err := WriteAsJSON(restorePoints, os.Stdout, pretty)
			tracelog.ErrorLogger.FatalOnError(err)
		case pretty:
			WritePrettyBackupList(restorePoints, os.Stdout)
		default:
			WriteBackupList(restorePoints, os.Stdout)
		}
	}
	logging := Logging{
		InfoLogger:  tracelog.InfoLogger,
		ErrorLogger: tracelog.ErrorLogger,
	}

	HandleBackupList(getRestorePointsFunc, writeRestorePointsListFunc, logging)
}
