package internal

import (
	"fmt"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"sort"
	"strings"
	"time"
)

const (
	NoDeleteModifier = iota
	FullDeleteModifier
	FindFullDeleteModifier
)

// TODO : unit tests
func GetLatestBackupName(folder storage.Folder) (string, error) {
	sortTimes, err := getBackups(folder)
	if err != nil {
		return "", err
	}

	return sortTimes[0].BackupName, nil
}

// TODO : unit tests
// getBackups receives backup descriptions and sorts them by time
func getBackups(folder storage.Folder) (backups []BackupTime, err error) {
	backups, _, err = getBackupsAndGarbage(folder)
	if err != nil {
		return nil, err
	}

	count := len(backups)
	if count == 0 {
		return nil, NewNoBackupsFoundError()
	}
	return
}

// TODO : unit tests
func getBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.GetSubFolder(BaseBackupPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := getBackupTimeSlices(backupObjects)
	garbage = getGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

// TODO : unit tests
func getBackupTimeSlices(backups []storage.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, SentinelSuffix) {
			continue
		}
		time := object.GetLastModified()
		sortTimes[i] = BackupTime{stripBackupName(key), time, stripWalFileName(key)}
	}
	slice := TimeSlice(sortTimes)
	sort.Sort(slice)
	return slice
}

// TODO : unit tests
func getGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
	garbage := make([]string, 0)
	var keyFilter = make(map[string]string)
	for _, k := range nonGarbage {
		keyFilter[k.BackupName] = k.BackupName
	}
	for _, folder := range folders {
		backupName := stripPrefixName(folder.GetPath())
		if _, ok := keyFilter[backupName]; ok {
			continue
		}
		garbage = append(garbage, backupName)
	}
	return garbage
}

func FindTargetBeforeName(folder storage.Folder,
	name string, modifier int,
	isFullBackup func(object storage.Object) bool,
	greater func(object1, object2 storage.Object) bool) (storage.Object, error) {

	choiceFunc := GetBeforeChoiceFunc(name, modifier, isFullBackup)
	if choiceFunc == nil {
		return nil, NewForbiddenActionError("Not allowed modifier for 'delete before'")
	}
	return FindTarget(folder, greater, choiceFunc)
}

func FindTargetBeforeTime(folder storage.Folder,
	timeLine time.Time, modifier int,
	isFullBackup func(object storage.Object) bool,
	less func(object1, object2 storage.Object) bool) (storage.Object, error) {

	potentialTarget, err := FindTarget(folder, less, func(object storage.Object) bool {
		return timeLine.Before(object.GetLastModified()) || timeLine.Equal(object.GetLastModified())
	})
	if err != nil {
		return nil, err
	}
	greater := func(object1, object2 storage.Object) bool {
		return less(object2, object1)
	}
	return FindTargetBeforeName(folder, potentialTarget.GetName(), modifier, isFullBackup, greater)
}

func FindTargetRetain(folder storage.Folder,
	retentionCount, modifier int,
	isFullBackup func(object storage.Object) bool,
	greater func(object1, object2 storage.Object) bool) (storage.Object, error) {

	choiceFunc := GetRetainChoiceFunc(retentionCount, modifier, isFullBackup)
	if choiceFunc == nil {
		return nil, NewForbiddenActionError("Not allowed modifier for 'delete retain'")
	}
	return FindTarget(folder, greater, choiceFunc)
}

func FindTarget(folder storage.Folder,
	compare func(object1, object2 storage.Object) bool,
	isTarget func(object storage.Object) bool) (storage.Object, error) {

	objects, _, err := folder.GetSubFolder(BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool {
		return compare(objects[i], objects[j])
	})
	for _, object := range objects {
		if isTarget(object) {
			return object, nil
		}
	}
	return nil, BackupNonExistenceError{}
}

func GetBeforeChoiceFunc(name string, modifier int,
	isFullBackup func(object storage.Object) bool) func(object storage.Object) bool {

	meetName := false
	switch modifier {
	case NoDeleteModifier:
		return func(object storage.Object) bool {
			return strings.HasPrefix(object.GetName(), name)
		}
	case FindFullDeleteModifier:
		return func(object storage.Object) bool {
			meetName = meetName || strings.HasPrefix(object.GetName(), name)
			return meetName && isFullBackup(object)
		}
	}
	return nil
}

func GetRetainChoiceFunc(retentionCount, modifier int,
	isFullBackup func(object storage.Object) bool) func(object storage.Object) bool {

	count := 0
	switch modifier {
	case NoDeleteModifier:
		return func(object storage.Object) bool {
			count++
			if count == retentionCount {
				return true
			}
			return false
		}
	case FullDeleteModifier:
		return func(object storage.Object) bool {
			if isFullBackup(object) {
				count++
			}
			if count == retentionCount {
				return true
			}
			return false
		}
	case FindFullDeleteModifier:
		return func(object storage.Object) bool {
			count++
			if count >= retentionCount && isFullBackup(object) {
				return true
			}
			return false
		}
	}
	return nil
}

func DeleteBeforeTarget(folder storage.Folder, target storage.Object,
	confirmed bool,
	isFullBackup func(object storage.Object) bool,
	less func(object1, object2 storage.Object) bool) error {

	if !isFullBackup(target) {
		errorMessage := "%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option."
		return NewForbiddenActionError(fmt.Sprintf(errorMessage, target.GetName()))
	}
	return storage.DeleteObjectsWhere(folder, confirmed, func(object storage.Object) bool {
		return less(object, target)
	})
}
