package greenplum

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type NoRestorePointsFoundError struct {
	error
}

func NewNoRestorePointsFoundError() NoRestorePointsFoundError {
	return NoRestorePointsFoundError{errors.New("No restore points found")}
}

// GetRestorePoints receives restore points descriptions and sorts them by time
func GetRestorePoints(folder storage.Folder) (restorePoints []internal.BackupTime, err error) {
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

func GetRestorePointsAndGarbage(folder storage.Folder) (restorePoints []internal.BackupTime, garbage []string, err error) {
	restorePointsObjects, subFolders, err := folder.ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := GetRestorePointsTimeSlices(restorePointsObjects)
	garbage = internal.GetGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

func GetRestorePointsTimeSlices(restorePoints []storage.Object) []internal.BackupTime {
	restorePointsTimes := make([]internal.BackupTime, 0)
	for _, object := range restorePoints {
		key := object.GetName()
		if !strings.HasSuffix(key, RestorePointSuffix) {
			continue
		}
		time := object.GetLastModified()
		restorePointsTimes = append(restorePointsTimes, internal.BackupTime{BackupName: StripRightmostRestorePointName(key), Time: time,
			WalFileName: utility.StripWalFileName(key)})
	}
	return restorePointsTimes
}

func StripRightmostRestorePointName(path string) string {
	path = strings.Trim(path, "/")
	all := strings.SplitAfter(path, "/")
	return stripRestorePointSuffix(all[len(all)-1])
}

func stripRestorePointSuffix(pathValue string) string {
	return strings.Split(pathValue, RestorePointSuffix)[0]
}
