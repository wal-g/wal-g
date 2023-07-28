package greenplum

import (
	"fmt"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type RestorePointBackupSelector struct {
	restorePoint string
}

func NewRestorePointBackupSelector(restorePoint string) *RestorePointBackupSelector {
	return &RestorePointBackupSelector{restorePoint: restorePoint}
}

func (s *RestorePointBackupSelector) Select(folder storage.Folder) (internal.Backup, error) {
	restorePoint, err := FetchRestorePointMetadata(folder, s.restorePoint)
	if err != nil {
		return internal.Backup{}, err
	}

	backups, err := ListStorageBackups(folder)
	if err != nil {
		return internal.Backup{}, err
	}

	// pick the latest (closest) backup to the restore point
	for i := len(backups) - 1; i >= 0; i-- {
		if backups[i].SentinelDto.FinishTime.Before(restorePoint.FinishTime) {
			return backups[i].Backup, nil
		}
	}

	return internal.Backup{}, fmt.Errorf(
		"failed to find matching backup (earlier than the finish time %s of the restore point %s)",
		restorePoint.Name, restorePoint.FinishTime)
}
