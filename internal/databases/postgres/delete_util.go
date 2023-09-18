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

			startWalSegmentNo := newWalSegmentNo(meta.StartLsn - 1)
			endWalSegmentNo := newWalSegmentNo(meta.FinishLsn - 1)
			for walSegmentNo := startWalSegmentNo; walSegmentNo <= endWalSegmentNo; walSegmentNo = walSegmentNo.next() {
				walObj := PermanentObject{
					Name:        walSegmentNo.getFilename(timelineID),
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
	if strings.HasPrefix(objectName, utility.WalPath) && len(objectName) >= len(utility.WalPath)+24 {
		wal := PermanentObject{
			Name:        objectName[len(utility.WalPath) : len(utility.WalPath)+24],
			StorageName: storageName,
		}
		return permanentWals[wal]
	}
	if strings.HasPrefix(objectName, utility.BaseBackupPath) {
		backup := PermanentObject{
			Name:        utility.StripLeftmostBackupName(objectName[len(utility.BaseBackupPath):]),
			StorageName: storageName,
		}
		return permanentBackups[backup]
	}
	// should not reach here, default to false
	return false
}
