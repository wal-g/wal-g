package archive

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// EnrichWithAttachedTS adds metadata from an attached TS sentinel to a main backup.
// Missing attached sentinels are valid for non-tiered backups.
func EnrichWithAttachedTS(ctx context.Context, folder storage.Folder, backup *Backup) error {
	if backup.BackupType == TSBackupType {
		backup.HasTS = true
		return nil
	}

	baseBackups := folder.GetSubFolder(utility.BaseBackupPath)
	sentinelPath := AttachedTSSentinelName(backup.BackupName)
	exists, err := baseBackups.Exists(ctx, sentinelPath)
	if err != nil {
		return fmt.Errorf("check attached ts sentinel for backup %s: %w", backup.BackupName, err)
	}
	if !exists {
		return nil
	}

	var tsBackup Backup
	if err := internal.FetchDto(ctx, baseBackups, &tsBackup, sentinelPath); err != nil {
		return fmt.Errorf("fetch attached ts sentinel for backup %s: %w", backup.BackupName, err)
	}

	backup.HasTS = true
	backup.TSBackupID = tsBackup.TSBackupID
	backup.TSBackupPath = tsBackup.TSBackupPath
	backup.TSDataSize = tsBackup.TSDataSize
	backup.TSFileCount = tsBackup.TSFileCount
	backup.TSStartTime = tsBackup.TSStartTime
	backup.TSFinishTime = tsBackup.TSFinishTime
	return nil
}
