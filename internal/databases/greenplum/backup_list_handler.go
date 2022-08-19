package greenplum

import (
	"fmt"
	"sort"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

//TODO: Implement backup-list handler

// ListStorageBackups returns the list of storage backups sorted by finish time (in ascending order)
func ListStorageBackups(folder storage.Folder) ([]Backup, error) {
	backupObjects, err := internal.GetBackups(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch list of backups in storage: %w", err)
	}

	backups := make([]Backup, 0, len(backupObjects))
	for _, b := range backupObjects {
		backup := NewBackup(folder, b.BackupName)

		_, err = backup.GetSentinel()
		if err != nil {
			return nil, fmt.Errorf("failed to load sentinel for backup %s: %w", b.BackupName, err)
		}

		backups = append(backups, backup)
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].SentinelDto.FinishTime.Before(backups[j].SentinelDto.FinishTime)
	})

	return backups, nil
}
