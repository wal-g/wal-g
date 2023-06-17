package postgres

import (
	"strings"
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

	lessFunc := makeLessFunc()
	postgresBackups, err := makeBackupObjects(folder, backups)
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
	isFullBackup bool, creationTime time.Time, object storage.Object, lsn LSN) BackupObject {
	return BackupObject{
		Object:            object,
		isFullBackup:      isFullBackup,
		baseBackupName:    incrementBase,
		incrementFromName: incrementFrom,
		creationTime:      creationTime,
		BackupName:        DeduceBackupName(object),
	}
}

type BackupObject struct {
	storage.Object
	BackupName        string
	isFullBackup      bool
	baseBackupName    string
	incrementFromName string
	creationTime      time.Time
	lsn               LSN
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
	folder storage.Folder, objects []storage.Object) ([]internal.BackupObject, error) {
	backupObjects := make([]internal.BackupObject, 0, len(objects))
	for _, object := range objects {
		incrementBase, incrementFrom, isFullBackup, lsn, err := getIncrementInfo(folder, object)
		if err != nil {
			return nil, err
		}

		postgresBackup := newBackupObject(
			incrementBase, incrementFrom, isFullBackup, object.GetLastModified(), object, lsn)

		backupObjects = append(backupObjects, postgresBackup)
	}
	return backupObjects, nil
}

func makePermanentFunc(permanentBackups, permanentWals map[string]bool) func(object storage.Object) bool {
	return func(object storage.Object) bool {
		return IsPermanent(object.GetName(), permanentBackups, permanentWals)
	}
}

func makeLessFunc() func(object1, object2 storage.Object) bool {
	return func(object1, object2 storage.Object) bool {
		time1, ok := utility.TryFetchTimeRFC3999(object1.GetName())
		if !ok {
			time1 = object1.GetLastModified().Format(utility.BackupTimeFormat)
		}
		time2, ok := utility.TryFetchTimeRFC3999(object2.GetName())
		if !ok {
			time2 = object2.GetLastModified().Format(utility.BackupTimeFormat)
		}
		return time1 < time2
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

func getIncrementInfo(folder storage.Folder, object storage.Object) (string, string, bool, LSN, error) {
	backup := NewBackup(folder.GetSubFolder(utility.BaseBackupPath), DeduceBackupName(object))
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return "", "", true, LSN(0), err
	}
	if !sentinel.IsIncremental() {
		return "", "", true, LSN(0), nil
	}

	return *sentinel.IncrementFullName, *sentinel.IncrementFrom, false, *sentinel.BackupStartLSN, nil
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

	target, err := dh.FindTargetByName(oldestBackup)
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
