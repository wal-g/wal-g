package internal

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type BackupTimeSlicesOrder int

const (
	ByCreationTime BackupTimeSlicesOrder = iota
	ByModificationTime
)

type NoBackupsFoundError struct {
	error
}

type TimedBackup interface {
	Name() string
	StartTime() time.Time
	IsPermanent() bool
}

func SortTimedBackup(backups []TimedBackup) {
	sort.Slice(backups, func(i, j int) bool {
		b1 := backups[i]
		b2 := backups[j]
		return b1.StartTime().After(b2.StartTime())
	})
}

func NewNoBackupsFoundError() NoBackupsFoundError {
	return NoBackupsFoundError{errors.New("No backups found")}
}

func (err NoBackupsFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func GetLatestBackupName(folder storage.Folder) (string, error) {
	backupTimes, err := GetBackups(folder)
	SortBackupTimeSlices(backupTimes)
	if err != nil {
		return "", err
	}

	return backupTimes[len(backupTimes)-1].BackupName, nil
}

func GetBackupSentinelObjects(folder storage.Folder) ([]storage.Object, error) {
	objects, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	sentinelObjects := make([]storage.Object, 0, len(objects))
	for _, object := range objects {
		if !strings.HasSuffix(object.GetName(), utility.SentinelSuffix) {
			continue
		}
		sentinelObjects = append(sentinelObjects, object)
	}

	return sentinelObjects, nil
}

// TODO : unit tests
// GetBackups receives backup descriptions and sorts them by time
func GetBackups(folder storage.Folder) (backups []BackupTime, err error) {
	backups, _, err = GetBackupsAndGarbage(folder)
	if err != nil {
		return nil, err
	}

	count := len(backups)
	if count == 0 {
		return nil, NewNoBackupsFoundError()
	}
	return
}

// GetBackupsWithMetadata receives backup descriptions with meta information
func GetBackupsWithMetadata(folder storage.Folder, metaFetcher GenericMetaFetcher) (backupsWithMeta []BackupTimeWithMetadata, err error) {
	backups, err := GetBackups(folder)

	if err != nil {
		return nil, err
	}

	backupsWithMeta = make([]BackupTimeWithMetadata, len(backups))
	for i, backup := range backups {
		meta, err := metaFetcher.Fetch(backup.BackupName, folder)
		if err != nil {
			return nil, err
		}

		backupsWithMeta[i] = BackupTimeWithMetadata{backup, meta}
	}
	return
}

// TODO : unit tests
func GetBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := GetBackupTimeSlices(backupObjects)
	garbage = GetGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

func GetBackupTimeSlices(backups []storage.Object) []BackupTime {
	backupTimes := make([]BackupTime, 0)
	for _, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		time := object.GetLastModified()
		backupTimes = append(backupTimes, BackupTime{utility.StripRightmostBackupName(key), time,
			utility.StripWalFileName(key)})
	}
	return backupTimes
}

func SortBackupTimeSlices(backupTimes []BackupTime) {
	sort.Slice(backupTimes, func(i, j int) bool {
		return backupTimes[i].Time.Before(backupTimes[j].Time)
	})
}

func SortBackupTimeWithMetadataSlices(backupTimes []BackupTimeWithMetadata) {
	order := ByCreationTime

	for i := 0; i < len(backupTimes); i++ {
		if (backupTimes[i].StartTime == time.Time{}) {
			order = ByModificationTime
			break
		}
	}

	if order == ByCreationTime {
		sort.Slice(backupTimes, func(i, j int) bool {
			return backupTimes[i].StartTime.Before(backupTimes[j].StartTime)
		})
	} else {
		sort.Slice(backupTimes, func(i, j int) bool {
			return backupTimes[i].Time.Before(backupTimes[j].Time)
		})
	}
}

func GetGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
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

func SentinelNameFromBackup(backupName string) string {
	return backupName + utility.SentinelSuffix
}

func MetadataNameFromBackup(backupName string) string {
	return backupName + "/" + utility.MetadataFileName
}

func StreamMetadataNameFromBackup(backupName string) string {
	return backupName + "/" + utility.StreamMetadataFileName
}

// UnwrapLatestModifier checks if LATEST is provided instead of backupName
// if so, replaces it with the name of the latest backup
func UnwrapLatestModifier(backupName string, folder storage.Folder) (string, error) {
	if backupName != LatestString {
		return backupName, nil
	}

	latest, err := GetLatestBackupName(folder)
	if err != nil {
		return "", err
	}
	tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)
	return latest, nil
}

func FolderSize(folder storage.Folder, path string) (int64, error) {
	dataObjects, _, err := folder.GetSubFolder(path).ListFolder()
	if err != nil {
		return 0, err
	}
	var size int64
	for _, obj := range dataObjects {
		size += obj.GetSize()
	}
	return size, nil
}

// SplitPurgingBackups partitions backups to delete and retain
func SplitPurgingBackups(backups []TimedBackup,
	retainCount *int,
	retainAfter *time.Time) (purge, retain map[string]bool, err error) {
	retain = make(map[string]bool)
	purge = make(map[string]bool)

	retainedCount := 0
	for i := range backups {
		backup := backups[i]
		if backup.IsPermanent() {
			tracelog.DebugLogger.Printf("Preserving backup due to keep permanent policy: %s", backup.Name())
			retain[backup.Name()] = true
			continue
		}

		if retainCount != nil && retainedCount < *retainCount { // TODO: fix condition, use func args
			retainedCount++
			tracelog.DebugLogger.Printf("Preserving backup due to retain count policy [%d/%d]: %s",
				retainedCount, *retainCount, backup.Name())
			retain[backup.Name()] = true
			continue
		}

		if retainAfter != nil && backup.StartTime().After(*retainAfter) { // TODO: fix condition, use func args
			tracelog.DebugLogger.Printf("Preserving backup due to retain time policy: %s", backup.Name())
			retain[backup.Name()] = true
			continue
		}
		purge[backup.Name()] = true
	}
	return purge, retain, nil
}

// DeleteGarbage purges given garbage keys
func DeleteGarbage(folder storage.Folder, garbage []string) error {
	var keys []string
	for _, prefix := range garbage {
		garbageObjects, _, err := folder.GetSubFolder(prefix).ListFolder()
		if err != nil {
			return err
		}
		for _, obj := range garbageObjects {
			keys = append(keys, path.Join(prefix, obj.GetName()))
		}
	}
	tracelog.DebugLogger.Printf("Garbage keys will be deleted: %+v\n", keys)
	return folder.DeleteObjects(keys)
}

// DeleteBackups purges given backups files
// TODO: extract BackupLayout abstraction and provide DataPath(), SentinelPath(), Exists() methods
func DeleteBackups(folder storage.Folder, backups []string) error {
	keys := make([]string, 0, len(backups)*2)
	for i := range backups {
		backupName := backups[i]
		keys = append(keys, SentinelNameFromBackup(backupName))

		dataObjects, _, err := folder.GetSubFolder(backupName).ListFolder()
		if err != nil {
			return err
		}
		for _, obj := range dataObjects {
			keys = append(keys, path.Join(backupName, obj.GetName()))
		}
	}

	tracelog.DebugLogger.Printf("Backup keys will be deleted: %+v\n", keys)
	if err := folder.DeleteObjects(keys); err != nil {
		return err
	}
	return nil
}
