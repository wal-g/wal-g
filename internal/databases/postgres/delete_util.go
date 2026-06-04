package postgres

import (
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type PermanentObject struct {
	Name        string
	StorageName string
}

func GetPermanentBackupsAndWals(folder storage.Folder) (map[PermanentObject]bool, map[PermanentObject]bool) {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	backupTimes, err := internal.GetBackups(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return map[PermanentObject]bool{}, map[PermanentObject]bool{}
	}

	backupsFolder := folder.GetSubFolder(utility.BaseBackupPath)

	permanentBackups := map[PermanentObject]bool{}
	permanentWals := map[PermanentObject]bool{}
	for _, backupTime := range backupTimes {
		backup, err := NewBackupInStorage(backupsFolder, backupTime.BackupName, backupTime.StorageName)
		if err != nil {
			internal.FatalOnUnrecoverableMetadataError(backupTime, err)
			continue
		}
		meta, err := backup.FetchMeta()
		if err != nil {
			internal.FatalOnUnrecoverableMetadataError(backupTime, err)
			continue
		}
		if meta.IsPermanent {
			timelineID, err := ParseTimelineFromBackupName(backup.Name)
			if err != nil {
				tracelog.ErrorLogger.Printf("failed to parse backup timeline for backup %s with error %s, ignoring...",
					backupTime.BackupName, err.Error())
				continue
			}

			startWalSegmentNo := NewWalSegmentNo(meta.StartLsn - 1)
			endWalSegmentNo := NewWalSegmentNo(meta.FinishLsn - 1)

			for walSegmentNo := startWalSegmentNo; walSegmentNo <= endWalSegmentNo; walSegmentNo = walSegmentNo.Next() {
				walObj := PermanentObject{
					Name:        walSegmentNo.GetFilename(timelineID),
					StorageName: backupTime.StorageName,
				}
				permanentWals[walObj] = true
			}
			backupObj := PermanentObject{
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

func IsPermanent(objectName, storageName string, permanentBackups, permanentWals map[PermanentObject]bool) bool {
	if walName, ok := walNameFromObjectPath(objectName); ok {
		wal := PermanentObject{
			Name:        walName,
			StorageName: storageName,
		}
		return permanentWals[wal]
	}
	if backupName, ok := backupNameFromObjectPath(objectName); ok {
		backup := PermanentObject{
			Name:        backupName,
			StorageName: storageName,
		}
		return permanentBackups[backup]
	}
	return false
}

func backupNameFromObjectPath(objectName string) (string, bool) {
	var relativePath string
	switch {
	case strings.HasPrefix(objectName, utility.BaseBackupPath):
		relativePath = objectName[len(utility.BaseBackupPath):]
	default:
		// Objects listed from the base backup subfolder have paths relative to that folder.
		relativePath = objectName
	}
	backupName := utility.StripLeftmostBackupName(relativePath)
	if backupName == "" {
		return "", false
	}
	return backupName, true
}

func walNameFromObjectPath(objectName string) (string, bool) {
	switch {
	case strings.HasPrefix(objectName, utility.WalPath) && len(objectName) >= len(utility.WalPath)+24:
		return objectName[len(utility.WalPath) : len(utility.WalPath)+24], true
	case strings.HasPrefix(objectName, utility.WalPath):
		return "", false
	default:
		// WAL objects listed from the WAL subfolder have paths relative to that folder.
		walName := utility.StripWalFileName(objectName)
		if walName == strings.Repeat("Z", 24) {
			return "", false
		}
		return walName, true
	}
}
