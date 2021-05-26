package postgres

import (
	"sort"
	"time"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
)

type BackupTimeSlicesOrder int

const (
	ByCreationTime BackupTimeSlicesOrder = iota
	ByModificationTime
)

func GetBackupsDetails(folder storage.Folder, backups []internal.BackupTime) ([]BackupDetail, error) {
	backupsDetails := make([]BackupDetail, 0, len(backups))
	for i := len(backups) - 1; i >= 0; i-- {
		details, err := GetBackupDetails(folder, backups[i])
		if err != nil {
			return nil, err
		}
		backupsDetails = append(backupsDetails, details)
	}
	return backupsDetails, nil
}

func GetBackupDetails(folder storage.Folder, backupTime internal.BackupTime) (BackupDetail, error) {
	backup := NewBackup(folder, backupTime.BackupName)

	metaData, err := backup.FetchMeta()
	if err != nil {
		return BackupDetail{}, err
	}
	return BackupDetail{backupTime, metaData}, nil
}

func SortBackupDetails(backupDetails []BackupDetail) {
	sortOrder := ByCreationTime
	for i := 0; i < len(backupDetails); i++ {
		if (backupDetails[i].ExtendedMetadataDto == ExtendedMetadataDto{} || backupDetails[i].StartTime == time.Time{}) {
			sortOrder = ByModificationTime
		}
	}
	if sortOrder == ByCreationTime {
		sort.Slice(backupDetails, func(i, j int) bool {
			return backupDetails[i].StartTime.Before(backupDetails[j].StartTime)
		})
	} else {
		sort.Slice(backupDetails, func(i, j int) bool {
			return backupDetails[i].Time.Before(backupDetails[j].Time)
		})
	}
}
