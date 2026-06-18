package postgres

import (
	"context"
	"slices"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type BackupTimeSlicesOrder int

const (
	ByCreationTime BackupTimeSlicesOrder = iota
	ByModificationTime
)

func GetBackupsDetails(ctx context.Context, folder storage.Folder, backups []internal.BackupTime) ([]BackupDetail, error) {
	backupsDetails := make([]BackupDetail, 0, len(backups))
	for i := len(backups) - 1; i >= 0; i-- {
		details, err := GetBackupDetails(ctx, folder, backups[i])
		if err != nil {
			return nil, err
		}
		backupsDetails = append(backupsDetails, details)
	}
	return backupsDetails, nil
}

func GetBackupDetails(ctx context.Context, folder storage.Folder, backupTime internal.BackupTime) (BackupDetail, error) {
	backup, err := NewBackupInStorage(ctx, folder, backupTime.BackupName, backupTime.StorageName)
	if err != nil {
		return BackupDetail{}, err
	}

	metaData, err := backup.FetchMeta(ctx)
	if err != nil {
		return BackupDetail{}, err
	}
	return BackupDetail{backupTime, metaData}, nil
}

func SortBackupDetails(backupDetails []BackupDetail) {
	sortOrder := ByCreationTime
	for i := 0; i < len(backupDetails); i++ {
		if (backupDetails[i].ExtendedMetadataDto == ExtendedMetadataDto{} || backupDetails[i].StartTime.Equal(time.Time{})) {
			sortOrder = ByModificationTime
		}
	}
	if sortOrder == ByCreationTime {
		slices.SortFunc(backupDetails, func(a, b BackupDetail) int {
			return a.StartTime.Compare(b.StartTime)
		})
	} else {
		slices.SortFunc(backupDetails, func(a, b BackupDetail) int {
			return a.Time.Compare(b.Time)
		})
	}
}
