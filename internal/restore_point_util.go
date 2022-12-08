package internal

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func NewNoRestorePointsFoundError() NoBackupsFoundError {
	return NoBackupsFoundError{errors.New("No restore points found")}
}

// GetRestorePoints receives restore points descriptions and sorts them by time
func GetRestorePoints(folder storage.Folder) (restorePoints []BackupTime, err error) {
	restorePoints, _, err = GetRestorePointsAndGarbage(folder)
	if err != nil {
		return nil, err
	}

	count := len(restorePoints)
	if count == 0 {
		return nil, NewNoRestorePointsFoundError()
	}
	return
}

func GetRestorePointsAndGarbage(folder storage.Folder) (restorePoints []BackupTime, garbage []string, err error) {
	restorePointsObjects, subFolders, err := folder.ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := GetRestorePointsTimeSlices(restorePointsObjects)
	garbage = GetGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

func GetRestorePointsTimeSlices(restorePoints []storage.Object) []BackupTime {
	restorePointsTimes := make([]BackupTime, 0)
	for _, object := range restorePoints {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.RestorePointSuffix) {
			continue
		}
		time := object.GetLastModified()
		restorePointsTimes = append(restorePointsTimes, BackupTime{utility.StripRightmostRestorePointName(key), time,
			utility.StripWalFileName(key)})
	}
	return restorePointsTimes
}
