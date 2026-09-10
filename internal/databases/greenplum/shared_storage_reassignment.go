package greenplum

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func listBackupsInOrder(ctx context.Context, baseBackupsFolder storage.Folder) ([]internal.BackupTime, error) {
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return nil, err
	}

	backups := internal.GetBackupTimeSlices(backupObjects)
	internal.SortBackupTimeSlices(backups)
	return backups, nil
}

// backupsWithChangedPredecessor returns surviving backups whose immediately preceding survivor
// changed between the two ordered snapshots. These are the only backups whose shared-storage
// ownership changes after deletion.
func backupsWithChangedPredecessor(before, after []internal.BackupTime) []string {
	previousBefore := make(map[string]string, len(before))
	previous := ""
	for _, backup := range before {
		previousBefore[backup.BackupName] = previous
		previous = backup.BackupName
	}

	affected := make([]string, 0)
	previous = ""
	for _, backup := range after {
		oldPrevious, existedBefore := previousBefore[backup.BackupName]
		if existedBefore && oldPrevious != previous {
			affected = append(affected, backup.BackupName)
		}
		previous = backup.BackupName
	}
	return affected
}
