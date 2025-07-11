package internal

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
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

func FilterOutNoBackupFoundError(err error, json bool) error {
	if _, isNoBackupsErr := err.(NoBackupsFoundError); isNoBackupsErr {
		// Having zero backups is not an error that should be handled in most cases.
		if !json {
			tracelog.InfoLogger.Println("No backups found")
		}
		return nil
	}
	return err
}

func (err NoBackupsFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func GetBackupByName(backupName, subfolder string, folder storage.Folder) (backup Backup, err error) {
	baseBackupFolder := folder.GetSubFolder(subfolder)

	if backupName == LatestString {
		return GetLatestBackup(baseBackupFolder)
	}

	return GetSpecificBackup(baseBackupFolder, backupName)
}

func GetLatestBackup(folder storage.Folder) (backup Backup, err error) {
	backupTimes, err := GetBackups(folder)
	if err != nil {
		return Backup{}, err
	}
	SortBackupTimeSlices(backupTimes)

	latest := backupTimes[len(backupTimes)-1]
	tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest.BackupName)

	return NewBackupInStorage(folder, latest.BackupName, latest.StorageName)
}

func GetSpecificBackup(folder storage.Folder, name string) (Backup, error) {
	sentinelName := SentinelNameFromBackup(name)
	exists, storageName, err := multistorage.Exists(folder, sentinelName)
	if err != nil {
		return Backup{}, fmt.Errorf("checking sentinel file %q for existence: %w", sentinelName, err)
	}
	if !exists {
		return Backup{}, NewBackupNonExistenceError(name)
	}
	return NewBackupInStorage(folder, name, storageName)
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

// GetBackups receives all backup descriptions from the folder.
func GetBackups(folder storage.Folder) (backups []BackupTime, err error) {
	backupObjects, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}

	backups = GetBackupTimeSlices(backupObjects)

	count := len(backups)
	if count == 0 {
		return nil, NewNoBackupsFoundError()
	}
	return
}

func GetBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.ListFolder()
	if err != nil {
		return nil, nil, err
	}

	backupTimes := GetBackupTimeSlices(backupObjects)
	garbage = GetGarbageFromPrefix(subFolders, backupTimes)

	return backupTimes, garbage, nil
}

func GetBackupTimeSlices(backupObjects []storage.Object) []BackupTime {
	backupTimes := make([]BackupTime, 0)
	for _, object := range backupObjects {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		storageName := multistorage.GetStorage(object)
		modTime := object.GetLastModified()
		backupTimes = append(backupTimes, BackupTime{utility.StripRightmostBackupName(key), modTime,
			utility.StripWalFileName(key), storageName})
	}
	return backupTimes
}

func SortBackupTimeSlices(backupTimes []BackupTime) {
	sort.Slice(backupTimes, func(i, j int) bool {
		return backupTimes[i].Time.Before(backupTimes[j].Time)
	})
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

// UnwrapLatestModifier checks if LATEST is provided instead of BackupName
// if so, replaces it with the name of the latest backup
func UnwrapLatestModifier(backupName string, folder storage.Folder) (string, error) {
	if backupName != LatestString {
		return backupName, nil
	}

	latest, err := GetLatestBackup(folder)
	if err != nil {
		return "", err
	}
	tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)
	return latest.Name, nil
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

// SplitPurgingBackups partitions backups to delete and retain, if no retains policy than retain all backups
func SplitPurgingBackups(backups []TimedBackup,
	retainCount *int,
	retainAfter *time.Time) (purge, retain map[string]bool, err error) {
	retain = make(map[string]bool)
	purge = make(map[string]bool)
	retainAll := retainCount == nil && retainAfter == nil
	retainedCount := 0
	for i := range backups {
		backup := backups[i]
		if backup.IsPermanent() {
			tracelog.DebugLogger.Printf("Preserving backup due to keep permanent policy: %s", backup.Name())
			retain[backup.Name()] = true
			continue
		}

		if retainAll {
			tracelog.DebugLogger.Printf("Preserving backup due to an unspecified policy: %s", backup.Name())
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
		garbageObjects, err := storage.ListFolderRecursively(folder.GetSubFolder(prefix))
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

		dataObjects, err := storage.ListFolderRecursively(folder.GetSubFolder(backupName))
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
