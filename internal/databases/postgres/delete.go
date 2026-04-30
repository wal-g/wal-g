package postgres

import (
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type DeleteHandler struct {
	internal.DeleteHandler
}

// HandleDeleteBefore deletes backups before a target, with snapshot cleanup
func (dh *DeleteHandler) HandleDeleteBefore(args []string, confirmed bool) {
	modifier, beforeStr := internal.ExtractDeleteModifierFromArgs(args)

	target, err := dh.FindTargetBefore(beforeStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		return
	}

	// Collect backups to delete for snapshot cleanup
	backupsToDelete := dh.collectBackupsBeforeTarget(target)
	handleSnapshotDeletion(backupsToDelete, dh.Folder)

	// Delegate to parent
	err = dh.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

// HandleDeleteRetain deletes old backups with retention, with snapshot cleanup
func (dh *DeleteHandler) HandleDeleteRetain(args []string, confirmed bool) {
	modifier, retentionStr := internal.ExtractDeleteModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionStr)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := dh.FindTargetRetain(retentionCount, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		return
	}

	// Collect backups to delete for snapshot cleanup
	backupsToDelete := dh.collectBackupsBeforeTarget(target)
	handleSnapshotDeletion(backupsToDelete, dh.Folder)

	// Delegate to parent
	err = dh.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

// HandleDeleteTarget deletes a specific backup, with snapshot cleanup
func (dh *DeleteHandler) HandleDeleteTarget(backupSelector internal.BackupSelector, confirmed bool, findFull bool) {
	backup, err := backupSelector.Select(dh.Folder)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := dh.FindTargetByName(backup.Name)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		return
	}

	// Collect backups to delete for snapshot cleanup
	backupsToDelete := dh.collectBackupsForTarget(target, findFull)
	handleSnapshotDeletion(backupsToDelete, dh.Folder)

	// Delegate to parent
	folderFilter := func(string) bool { return true }
	err = dh.DeleteTarget(target, confirmed, findFull, folderFilter)
	tracelog.ErrorLogger.FatalOnError(err)
}

// collectBackupsBeforeTarget collects backup names before a target
func (dh *DeleteHandler) collectBackupsBeforeTarget(target internal.BackupObject) []string {
	tracelog.InfoLogger.Printf("collectBackupsBeforeTarget: target is %s", target.GetBackupName())
	backupNames := make([]string, 0)
	// Get all backup objects from the folder
	backupObjects, err := internal.GetBackupSentinelObjects(dh.Folder)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to list backups for snapshot cleanup: %v", err)
		return backupNames
	}

	tracelog.InfoLogger.Printf("Found %d backup sentinel objects", len(backupObjects))
	for _, obj := range backupObjects {
		backupName := DeduceBackupName(obj)
		tracelog.InfoLogger.Printf("  Checking object %s -> backup name: %s", obj.GetName(), backupName)
		if backupName == "" {
			tracelog.InfoLogger.Printf("    Skipped: backup name is empty")
			continue
		}
		// Check if this backup is before the target
		// Simple name comparison (both backups and snapshots have sortable names)
		if backupName < target.GetBackupName() {
			tracelog.InfoLogger.Printf("    %s < %s, adding to deletion list", backupName, target.GetBackupName())
			backupNames = append(backupNames, backupName)
		} else {
			tracelog.InfoLogger.Printf("    %s >= %s, not before target", backupName, target.GetBackupName())
		}
	}
	tracelog.InfoLogger.Printf("collectBackupsBeforeTarget: collected %d backups", len(backupNames))
	return backupNames
}

// collectBackupsForTarget collects backup names for a specific target
func (dh *DeleteHandler) collectBackupsForTarget(target internal.BackupObject, findFull bool) []string {
	// For now, just return the target itself
	// In the future, this could be expanded to handle delta backups
	return []string{target.GetBackupName()}
}

// handleSnapshotDeletion handles deletion of snapshot backups
func handleSnapshotDeletion(backupNames []string, folder storage.Folder) {
	tracelog.InfoLogger.Printf("handleSnapshotDeletion called with %d backups", len(backupNames))
	for _, name := range backupNames {
		tracelog.InfoLogger.Printf("  - %s", name)
	}
	if len(backupNames) == 0 {
		tracelog.InfoLogger.Println("No backups to check for snapshot deletion")
		return
	}
	HandleSnapshotBackupDeletion(backupNames, folder)
}

const (
	DeleteGarbageArchivesModifier = "ARCHIVES"
	DeleteGarbageBackupsModifier  = "BACKUPS"
)

func NewDeleteHandler(folder storage.Folder, permanentBackups, permanentWals map[PermanentObject]bool,
	useSentinelTime bool,
) (*DeleteHandler, error) {
	backupSentinels, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	lessFunc := timelineAndSegmentNoLess
	var startTimeByBackupName map[string]time.Time
	if useSentinelTime {
		// If all backups in storage have metadata, we will use backup start time from sentinel.
		// Otherwise, for example in case when we are dealing with some ancient backup without
		// metadata included, fall back to the default timeline and segment number comparator.
		startTimeByBackupName, err = getBackupStartTimeMap(folder, backupSentinels)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get sentinel backup start times: %v,"+
				" will fall back to timeline and segment number for ordering...\n", err)
		} else {
			lessFunc = makeLessFunc(startTimeByBackupName)
		}
	}
	postgresBackups, err := makeBackupObjects(folder, backupSentinels, startTimeByBackupName)
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

func newBackupObject(
	incrementBase, incrementFrom string,
	isFullBackup bool,
	creationTime time.Time,
	object storage.Object,
	storageName string,
) BackupObject {
	return BackupObject{
		Object:            object,
		isFullBackup:      isFullBackup,
		baseBackupName:    incrementBase,
		incrementFromName: incrementFrom,
		creationTime:      creationTime,
		BackupName:        DeduceBackupName(object),
		storageName:       storageName,
	}
}

var _ internal.BackupObject = BackupObject{}

type BackupObject struct {
	storage.Object
	BackupName        string
	isFullBackup      bool
	baseBackupName    string
	incrementFromName string
	creationTime      time.Time
	storageName       string
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

func (o BackupObject) GetStorage() string {
	return o.storageName
}

func makeBackupObjects(
	folder storage.Folder, objects []storage.Object, startTimeByBackupName map[string]time.Time,
) ([]internal.BackupObject, error) {
	backupObjects := make([]internal.BackupObject, 0, len(objects))
	for _, object := range objects {
		storageName := multistorage.GetStorage(object)
		incrementBase, incrementFrom, isFullBackup, err := getIncrementInfo(folder, object, storageName)
		if err != nil {
			return nil, err
		}
		postgresBackup := newBackupObject(
			incrementBase, incrementFrom, isFullBackup, object.GetLastModified(), object, storageName)

		if startTimeByBackupName != nil {
			postgresBackup.creationTime = startTimeByBackupName[postgresBackup.BackupName]
		}
		backupObjects = append(backupObjects, postgresBackup)
	}
	return backupObjects, nil
}

func makePermanentFunc(permanentBackups, permanentWals map[PermanentObject]bool) func(object storage.Object) bool {
	return func(object storage.Object) bool {
		storageName := multistorage.GetStorage(object)
		return IsPermanent(object.GetName(), storageName, permanentBackups, permanentWals)
	}
}

func makeLessFunc(startTimeByBackupName map[string]time.Time) func(storage.Object, storage.Object) bool {
	return func(object1 storage.Object, object2 storage.Object) bool {
		backupName1 := DeduceBackupName(object1)
		if backupName1 == "" {
			// we can't compare non-backup storage objects (probably WAL segments) by start time,
			// so use the segment number comparator instead
			return segmentNoLess(object1, object2)
		}
		backupName2 := DeduceBackupName(object2)
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

func getIncrementInfo(folder storage.Folder, object storage.Object, storageName string) (string, string, bool, error) {
	backup, err := NewBackupInStorage(folder.GetSubFolder(utility.BaseBackupPath), DeduceBackupName(object), storageName)
	if err != nil {
		return "", "", true, err
	}
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
func (dh *DeleteHandler) HandleDeleteGarbage(args []string, confirm bool) error {
	predicate := ExtractDeleteGarbagePredicate(args)
	backupSelector := internal.NewOldestNonPermanentSelector(NewGenericMetaFetcher())
	oldestBackup, err := backupSelector.Select(dh.Folder)
	if err != nil {
		if _, ok := err.(internal.NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find any non-permanent backups in storage. Not doing anything.")
			return nil
		}
		return err
	}

	target, err := dh.FindTargetByName(oldestBackup.Name)
	if err != nil {
		return err
	}

	folderFilter := func(string) bool { return true }
	return dh.DeleteBeforeTargetWhere(target, confirm, predicate, folderFilter)
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
		return strings.HasPrefix(object.GetName(), prefix)
	}
}
