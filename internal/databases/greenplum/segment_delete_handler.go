package greenplum

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type SegDeleteType int

const (
	SegDeleteBefore SegDeleteType = iota
	SegDeleteTarget
)

type SegDeleteHandler interface {
	Delete(backup SegBackup) error
}

type SegDeleteBeforeHandler struct {
	*postgres.DeleteHandler
	contentID int
	args      DeleteArgs
}

func NewSegDeleteHandler(rootFolder storage.Folder, contentID int, args DeleteArgs, delType SegDeleteType,
) (SegDeleteHandler, error) {
	segFolder := rootFolder.GetSubFolder(FormatSegmentStoragePrefix(contentID))

	permanentBackups, permanentWals := GetPermanentBackupsAndWals(rootFolder, contentID)

	segDeleteHandler, err := postgres.NewDeleteHandler(segFolder, permanentBackups, permanentWals, false)
	if err != nil {
		return nil, err
	}

	switch delType {
	case SegDeleteBefore:
		return &SegDeleteBeforeHandler{
			DeleteHandler: segDeleteHandler,
			contentID:     contentID,
			args:          args,
		}, nil

	case SegDeleteTarget:
		return &SegDeleteTargetHandler{
			DeleteHandler: segDeleteHandler,
			contentID:     contentID,
			args:          args,
		}, nil

	default:
		return nil, fmt.Errorf("unknown segment delete handler type")
	}
}

func (h SegDeleteBeforeHandler) Delete(segBackup SegBackup) error {
	segTarget, err := h.FindTargetByName(segBackup.Name)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("Running delete before target %s on segment %d\n",
		segTarget.GetBackupName(), h.contentID)

	filterFunc := func(object storage.Object) bool { return true }
	folderFilter := func(folderPath string) bool {
		aoSegFolderPrefix := path.Join(utility.BaseBackupPath, AoStoragePath)
		return !strings.HasPrefix(folderPath, aoSegFolderPrefix)
	}
	err = h.DeleteBeforeTargetWhere(segTarget, h.args.Confirmed, filterFunc, folderFilter)
	if err != nil {
		return err
	}

	return cleanupAOSegments(segTarget, h.Folder, h.args.Confirmed)
}

type SegDeleteTargetHandler struct {
	*postgres.DeleteHandler
	contentID int
	args      DeleteArgs
}

func (h SegDeleteTargetHandler) Delete(segBackup SegBackup) error {
	segTarget, err := h.FindTargetByName(segBackup.Name)
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Printf("Running delete target %s on segment %d\n",
		segTarget.GetBackupName(), h.contentID)

	folderFilter := func(folderPath string) bool {
		return !strings.HasPrefix(folderPath, AoStoragePath)
	}
	err = h.DeleteTarget(segTarget, h.args.Confirmed, h.args.FindFull, folderFilter)
	if err != nil {
		return err
	}

	return cleanupAOSegments(segTarget, h.Folder, h.args.Confirmed)
}

// TODO: unit tests
func cleanupAOSegments(target internal.BackupObject, segFolder storage.Folder, confirmed bool) error {
	aoSegFolder := segFolder.GetSubFolder(utility.BaseBackupPath).GetSubFolder(AoStoragePath)
	aoSegmentsToRetain, err := LoadStorageAoFiles(segFolder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return err
	}

	for segPath := range aoSegmentsToRetain {
		tracelog.DebugLogger.Printf("%s is still used, retaining...\n", segPath)
	}

	tracelog.InfoLogger.Printf("Cleaning up the AO segment objects")
	aoSegmentsToDelete, err := findAoSegmentsToDelete(target, aoSegmentsToRetain, aoSegFolder)
	if err != nil {
		return err
	}

	if !confirmed {
		return nil
	}

	return aoSegFolder.DeleteObjects(aoSegmentsToDelete)
}

func GetPermanentBackupsAndWals(rootFolder storage.Folder, contentID int) (map[postgres.PermanentObject]bool,
	map[postgres.PermanentObject]bool) {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	folder := rootFolder.GetSubFolder(FormatSegmentStoragePrefix(contentID))
	backupTimes, err := internal.GetBackups(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		tracelog.WarningLogger.Println("Error while fetching backups")
		return map[postgres.PermanentObject]bool{}, map[postgres.PermanentObject]bool{}
	}

	restorePointMetas, err := FetchAllRestorePoints(rootFolder)
	if err != nil {
		tracelog.WarningLogger.Println("Error while fetching restore points")
		return map[postgres.PermanentObject]bool{}, map[postgres.PermanentObject]bool{}
	}

	backupsFolder := folder.GetSubFolder(utility.BaseBackupPath)

	permanentBackups := map[postgres.PermanentObject]bool{}
	permanentWals := map[postgres.PermanentObject]bool{}
	for _, backupTime := range backupTimes {
		backup, err := postgres.NewBackupInStorage(backupsFolder, backupTime.BackupName, backupTime.StorageName)
		if err != nil {
			internal.FatalOnUnrecoverableMetadataError(backupTime, err)
			continue
		}

		meta, err := backup.FetchMeta()
		if err != nil {
			internal.FatalOnUnrecoverableMetadataError(backupTime, err)
			continue
		}

		restorePoint, err := FindRestorePointWithTS(meta.StartTime.Format(time.RFC3339), restorePointMetas)
		if err != nil {
			internal.FatalOnUnrecoverableMetadataError(backupTime, err)
			continue
		}

		restorePointMeta, err := FetchRestorePointMetadata(rootFolder, restorePoint)
		if err != nil {
			internal.FatalOnUnrecoverableMetadataError(backupTime, err)
			continue
		}

		if meta.IsPermanent {
			timelineID, err := postgres.ParseTimelineFromBackupName(backup.Name)
			if err != nil {
				tracelog.ErrorLogger.Printf("failed to parse backup timeline for backup %s with error %s, ignoring...",
					backupTime.BackupName, err.Error())
				continue
			}

			startWalSegmentNo := postgres.NewWalSegmentNo(meta.StartLsn - 1)
			lsn, err := postgres.ParseLSN(restorePointMeta.LsnBySegment[contentID])
			if err != nil {
				tracelog.ErrorLogger.Printf("failed to parse lsn  %v\n", err)
				continue
			}
			endWalSegmentNo := postgres.NewWalSegmentNo(lsn)
			tracelog.InfoLogger.Printf("permament wal from %s to %s\n",
				startWalSegmentNo.GetFilename(timelineID), endWalSegmentNo.GetFilename(timelineID))

			for walSegmentNo := startWalSegmentNo; walSegmentNo <= endWalSegmentNo; walSegmentNo = walSegmentNo.Next() {
				walObj := postgres.PermanentObject{
					Name:        walSegmentNo.GetFilename(timelineID),
					StorageName: backupTime.StorageName,
				}
				permanentWals[walObj] = true
			}
			backupObj := postgres.PermanentObject{
				Name:        backupTime.BackupName,
				StorageName: backupTime.StorageName,
			}
			permanentBackups[backupObj] = true
		}
	}
	if len(permanentBackups) > 0 {
		tracelog.InfoLogger.Printf("Found permanent objects: backups=%v, wals=%v\n",
			permanentBackups, permanentWals)
	}
	return permanentBackups, permanentWals
}

// TODO: unit tests
func findAoSegmentsToDelete(target internal.BackupObject,
	aoSegmentsToRetain map[string]struct{}, aoSegFolder storage.Folder) ([]string, error) {
	aoObjects, _, err := aoSegFolder.ListFolder()
	if err != nil {
		return nil, err
	}

	aoSegmentsToDelete := make([]string, 0)
	for _, obj := range aoObjects {
		if !strings.HasSuffix(obj.GetName(), AoSegSuffix) && obj.GetLastModified().After(target.GetLastModified()) {
			tracelog.DebugLogger.Println(
				"\tis not an AO segment file, will not delete (modify time is too recent): " + obj.GetName())
			continue
		}

		if _, ok := aoSegmentsToRetain[obj.GetName()]; ok {
			// this AO segment file is still referenced by some backup, skip it
			tracelog.DebugLogger.Println("\tis still referenced by some backups, will not delete: " + obj.GetName())
			continue
		}

		tracelog.InfoLogger.Println("\twill be deleted: " + obj.GetName())

		aoSegmentsToDelete = append(aoSegmentsToDelete, obj.GetName())
	}

	return aoSegmentsToDelete, nil
}
