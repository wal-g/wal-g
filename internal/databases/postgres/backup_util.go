package postgres

import (
	"sort"
	"strings"
	"time"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"github.com/wal-g/wal-g/internal"
)

type BackupTimeSlicesOrder int

const (
	ByCreationTime BackupTimeSlicesOrder = iota
	ByModificationTime
)

// TODO : unit tests
// GetBackups receives backup descriptions and sorts them by time
func GetBackups(folder storage.Folder) (backups []BackupTime, err error) {
	return GetBackupsWithTarget(folder, utility.BaseBackupPath)
}

func GetBackupsWithTarget(folder storage.Folder, targetPath string) (backups []BackupTime, err error) {
	backups, _, err = GetBackupsAndGarbageWithTarget(folder, targetPath)
	if err != nil {
		return nil, err
	}

	count := len(backups)
	if count == 0 {
		return nil, internal.NewNoBackupsFoundError()
	}
	return
}

func GetBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	return GetBackupsAndGarbageWithTarget(folder, utility.BaseBackupPath)
}

// TODO : unit tests
func GetBackupsAndGarbageWithTarget(folder storage.Folder, targetPath string) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.GetSubFolder(targetPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := GetBackupTimeSlices(backupObjects, folder)
	garbage = getGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

func SortBackupTimeSlices(backupsSlices *[]BackupTime, sortOrder BackupTimeSlicesOrder) {
	if sortOrder == ByCreationTime {
		sort.Slice(*backupsSlices, func(i, j int) bool {
			return (*backupsSlices)[i].CreationTime.After((*backupsSlices)[j].CreationTime)
		})
	} else {
		sort.Slice(*backupsSlices, func(i, j int) bool {
			return (*backupsSlices)[i].ModificationTime.After((*backupsSlices)[j].ModificationTime)
		})
	}
}

// TODO : unit tests
func GetBackupTimeSlices(backups []storage.Object, folder storage.Folder) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	sortOrder := ByCreationTime
	for i, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		metaData, err := GetBackupMetaData(folder, utility.StripRightmostBackupName(key), utility.BaseBackupPath)
		var creationTime time.Time = time.Time{}
		if (err == nil && metaData.StartTime != time.Time{}) {
			creationTime = metaData.StartTime
		} else {
			sortOrder = ByModificationTime
		}
		sortTimes[i] = BackupTime{utility.StripRightmostBackupName(key), creationTime, object.GetLastModified(), utility.StripWalFileName(key)}
	}
	SortBackupTimeSlices(&sortTimes, sortOrder)
	return sortTimes
}

// TODO : unit tests
func getGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
	garbage := make([]string, 0)
	var keyFilter = make(map[string]string)
	for _, k := range nonGarbage {
		keyFilter[k.BackupName] = k.BackupName
	}
	for _, folder := range folders {
		backupName := utility.StripPrefixName(folder.GetPath())
		if _, ok := keyFilter[backupName]; ok {
			continue
		}
		garbage = append(garbage, backupName)
	}
	return garbage
}
