package postgres

import (
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type DeleteHandler struct {
	internal.DeleteHandler
}

const (
	DeleteGarbageArchivesModifier = "ARCHIVES"
	DeleteGarbageBackupsModifier  = "BACKUPS"
)

func NewDeleteHandler(folder storage.Folder, permanentBackups, permanentWals map[string]bool,
	useSentinelTime bool,
) (*DeleteHandler, error) {
	backups, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	lessFunc := timelineAndSegmentNoLess
	var startTimeByBackupName map[string]time.Time
	if useSentinelTime {
		// If all backups in storage have metadata, we will use backup start time from sentinel.
		// Otherwise, for example in case when we are dealing with some ancient backup without
		// metadata included, fall back to the default timeline and segment number comparator.
		startTimeByBackupName, err = getBackupStartTimeMap(folder, backups)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get sentinel backup start times: %v,"+
				" will fall back to timeline and segment number for ordering...\n", err)
		} else {
			lessFunc = makeLessFunc(startTimeByBackupName)
		}
	}
	postgresBackups, err := makeBackupObjects(folder, backups, startTimeByBackupName)
	if err != nil {
		return nil, err
	}

	deleteHandler :=
		&DeleteHandler{
			*internal.NewDeleteHandler(
				folder,
				postgresBackups,
				lessFunc,
				internal.IsPermanentFunc(
					makePermanentFunc(permanentBackups, permanentWals))),
		}

	return deleteHandler, nil
}

func newBackupObject(incrementBase, incrementFrom string,
	isFullBackup bool, creationTime time.Time, object storage.Object) BackupObject {
	return BackupObject{
		Object:            object,
		isFullBackup:      isFullBackup,
		baseBackupName:    incrementBase,
		incrementFromName: incrementFrom,
		creationTime:      creationTime,
		BackupName:        FetchPgBackupName(object),
	}
}

type BackupObject struct {
	storage.Object
	BackupName        string
	isFullBackup      bool
	baseBackupName    string
	incrementFromName string
	creationTime      time.Time
}

func (o BackupObject) IsFullBackup() bool {
	return o.isFullBackup
}

func (o BackupObject) GetBaseBackupName() string {
	return o.baseBackupName
}

func (o BackupObject) GetBackupTime() time.Time {
	return o.creationTime
}

func (o BackupObject) GetBackupName() string {
	return o.BackupName
}

func (o BackupObject) GetIncrementFromName() string {
	return o.incrementFromName
}

func makeBackupObjects(
	folder storage.Folder, objects []storage.Object, startTimeByBackupName map[string]time.Time,
) ([]internal.BackupObject, error) {
	backupObjects := make([]internal.BackupObject, 0, len(objects))
	for _, object := range objects {
		incrementBase, incrementFrom, isFullBackup, err := getIncrementInfo(folder, object)
		if err != nil {
			return nil, err
		}
		postgresBackup := newBackupObject(
			incrementBase, incrementFrom, isFullBackup, object.GetLastModified(), object)

		if startTimeByBackupName != nil {
			postgresBackup.creationTime = startTimeByBackupName[postgresBackup.BackupName]
		}
		backupObjects = append(backupObjects, postgresBackup)
	}
	return backupObjects, nil
}

func makePermanentFunc(permanentBackups, permanentWals map[string]bool) func(object storage.Object) bool {
	return func(object storage.Object) bool {
		return IsPermanent(object.GetName(), permanentBackups, permanentWals)
	}
}

func makeLessFunc(startTimeByBackupName map[string]time.Time) func(storage.Object, storage.Object) bool {
	return func(object1 storage.Object, object2 storage.Object) bool {
		backupName1 := FetchPgBackupName(object1)
		if backupName1 == "" {
			// we can't compare non-backup storage objects (probably WAL segments) by start time,
			// so use the segment number comparator instead
			return segmentNoLess(object1, object2)
		}
		backupName2 := FetchPgBackupName(object2)
		if backupName2 == "" {
			return segmentNoLess(object1, object2)
		}

		startTime1, ok := startTimeByBackupName[backupName1]
		if !ok {
			return false
		}
		startTime2, ok := startTimeByBackupName[backupName2]
		if !ok {
			return false
		}
		return startTime1.Before(startTime2)
	}
}

// getBackupStartTimeMap returns a map for a fast lookup of the backup start time by the backup name
func getBackupStartTimeMap(folder storage.Folder, backups []storage.Object) (map[string]time.Time, error) {
	backupTimes := internal.GetBackupTimeSlices(backups)
	startTimeByBackupName := make(map[string]time.Time, len(backups))

	for _, backupTime := range backupTimes {
		backupDetails, err := GetBackupDetails(folder.GetSubFolder(utility.BaseBackupPath), backupTime)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get metadata of backup %s",
				backupTime.BackupName)
		}
		startTimeByBackupName[backupDetails.BackupName] = backupDetails.StartTime
	}
	return startTimeByBackupName, nil
}

func segmentNoLess(object1 storage.Object, object2 storage.Object) bool {
	_, segmentNumber1, ok := TryFetchTimelineAndLogSegNo(object1.GetName())
	if !ok {
		return false
	}
	_, segmentNumber2, ok := TryFetchTimelineAndLogSegNo(object2.GetName())
	if !ok {
		return false
	}
	return segmentNumber1 < segmentNumber2
}

func timelineAndSegmentNoLess(object1 storage.Object, object2 storage.Object) bool {
	tl1, segNo1, ok := TryFetchTimelineAndLogSegNo(object1.GetName())
	if !ok {
		return false
	}
	tl2, segNo2, ok := TryFetchTimelineAndLogSegNo(object2.GetName())
	if !ok {
		return false
	}
	return tl1 < tl2 || tl1 == tl2 && segNo1 < segNo2
}

func getIncrementInfo(folder storage.Folder, object storage.Object) (string, string, bool, error) {
	backup := NewBackup(folder.GetSubFolder(utility.BaseBackupPath), FetchPgBackupName(object))
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return "", "", true, err
	}
	if !sentinel.IsIncremental() {
		return "", "", true, nil
	}

	return *sentinel.IncrementFullName, *sentinel.IncrementFrom, false, nil
}

// HandleDeleteGarbage delete outdated WAL archives and leftover backup files
func (dh *DeleteHandler) HandleDeleteGarbage(args []string, folder storage.Folder, confirm bool) error {
	predicate := ExtractDeleteGarbagePredicate(args)
	oldestBackup, err := findOldestNonPermanentBackup(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		if _, ok := err.(internal.NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find any non-permanent backups in storage. Not doing anything.")
			return nil
		}
		return err
	}

	target, err := dh.FindTargetByName(oldestBackup.BackupName)
	if err != nil {
		return err
	}

	return dh.DeleteBeforeTargetWhere(target, confirm, predicate)
}

// ExtractDeleteGarbagePredicate extracts delete modifier the "delete garbage" command
func ExtractDeleteGarbagePredicate(args []string) func(storage.Object) bool {
	switch {
	case len(args) == 1 && args[0] == DeleteGarbageArchivesModifier:
		tracelog.InfoLogger.Printf("Archive-only mode selected. Will remove only outdated WAL files.")
		return storagePrefixFilter(utility.WalPath)
	case len(args) == 1 && args[0] == DeleteGarbageBackupsModifier:
		tracelog.InfoLogger.Printf("Backups-only mode selected. Will remove only leftover backup files.")
		return storagePrefixFilter(utility.BaseBackupPath)
	default:
		tracelog.InfoLogger.Printf("Running in default mode. Will remove outdated WAL files and leftover backup files.")
		return func(storage.Object) bool { return true }
	}
}

func storagePrefixFilter(prefix string) func(storage.Object) bool {
	return func(object storage.Object) bool {
		objectName := object.GetName()
		return len(objectName) >= len(prefix) && objectName[:len(prefix)] == prefix
	}
}

// findOldestNonPermanentBackup finds oldest non-permanent backup available in storage.
func findOldestNonPermanentBackup(
	folder storage.Folder,
) (*BackupDetail, error) {
	backups, err := internal.GetBackups(folder)
	if err != nil {
		// this also includes the zero backups case
		return nil, err
	}

	backupDetails, err := GetBackupsDetails(folder, backups)
	if err != nil {
		return nil, err
	}

	SortBackupDetails(backupDetails)

	for i := range backupDetails {
		currBackup := &backupDetails[i]

		if currBackup.IsPermanent {
			tracelog.InfoLogger.Printf(
				"Backup %s is permanent, it is not eligible to be selected "+
					"as the oldest backup for delete garbage.\n", currBackup.BackupName)
			continue
		}
		tracelog.InfoLogger.Printf("Found earliest non-permanent backup: %s\n", currBackup.BackupName)
		return currBackup, nil
	}

	return nil, internal.NewNoBackupsFoundError()
}
