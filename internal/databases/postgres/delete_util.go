package postgres

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func GetPermanentBackupsAndWals(folder storage.Folder) (map[string]bool, map[string]bool) {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	backupTimes, err := internal.GetBackups(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return map[string]bool{}, map[string]bool{}
	}

	permanentBackups := map[string]bool{}
	permanentWals := map[string]bool{}
	for _, backupTime := range backupTimes {
		backup := NewBackup(folder.GetSubFolder(utility.BaseBackupPath), backupTime.BackupName)
		meta, err := backup.FetchMeta()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to fetch backup meta for backup %s with error %s, ignoring...",
				backupTime.BackupName, err.Error())
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
				permanentWals[walSegmentNo.getFilename(timelineID)] = true
			}
			permanentBackups[backupTime.BackupName] = true
		}
	}
	return permanentBackups, permanentWals
}

func IsPermanent(objectName string, permanentBackups, permanentWals map[string]bool) bool {
	if objectName[:len(utility.WalPath)] == utility.WalPath {
		wal := objectName[len(utility.WalPath) : len(utility.WalPath)+24]
		return permanentWals[wal]
	}
	if objectName[:len(utility.BaseBackupPath)] == utility.BaseBackupPath {
		backup := utility.StripLeftmostBackupName(objectName[len(utility.BaseBackupPath):])
		return permanentBackups[backup]
	}
	// should not reach here, default to false
	return false
}
