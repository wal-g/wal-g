package postgres

import (
	"os"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// ExecuteSnapshotDeleteCommand executes the configured snapshot delete command for a backup
func ExecuteSnapshotDeleteCommand(backupName string) error {
	snapshotDeleteCmd, ok := GetSnapshotDeleteCommand()
	if !ok || snapshotDeleteCmd == "" {
		tracelog.DebugLogger.Printf("No snapshot delete command configured, skipping for backup %s", backupName)
		return nil
	}

	tracelog.InfoLogger.Printf("Executing snapshot delete command for backup: %s", backupName)

	cmd := exec.Command("/bin/sh", "-c", snapshotDeleteCmd)
	cmd.Env = append(os.Environ(),
		"WALG_SNAPSHOT_NAME="+backupName,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		tracelog.WarningLogger.Printf("Snapshot delete command failed for %s with output: %s", 
			backupName, string(output))
		return errors.Wrapf(err, "snapshot delete command execution failed: %s", string(output))
	}

	tracelog.InfoLogger.Printf("Snapshot delete command completed successfully for backup %s", backupName)
	tracelog.DebugLogger.Printf("Snapshot delete command output: %s", string(output))

	return nil
}

// IsSnapshotBackup checks if a backup is a snapshot backup by examining its sentinel
func IsSnapshotBackup(backupName string, sentinelDto BackupSentinelDto) bool {
	// Snapshot backups are identified by having FilesMetadataDisabled=true
	// and no increment information (they are always full backups)
	return sentinelDto.FilesMetadataDisabled && 
		sentinelDto.IncrementFrom == nil && 
		sentinelDto.CompressedSize == 0 && 
		sentinelDto.UncompressedSize == 0
}

// HandleSnapshotBackupDeletion handles deletion of snapshot backups
// This function should be called before the backup metadata is deleted
func HandleSnapshotBackupDeletion(backupNames []string, folder storage.Folder) {
	// Check if snapshot delete command is configured
	_, ok := conf.GetSetting(conf.PgSnapshotDeleteCommand)
	if !ok {
		tracelog.DebugLogger.Println("No snapshot delete command configured, skipping snapshot cleanup")
		return
	}

	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	
	for _, backupName := range backupNames {
		// Try to fetch the backup metadata to check if it's a snapshot backup
		backup, err := NewBackupInStorage(baseBackupFolder, backupName, "")
		if err != nil {
			tracelog.WarningLogger.Printf("Could not load backup %s metadata, skipping snapshot cleanup: %v", 
				backupName, err)
			continue
		}

		sentinel := backup.SentinelDto
		if sentinel == nil {
			tracelog.WarningLogger.Printf("Backup %s has no sentinel, skipping snapshot cleanup", backupName)
			continue
		}

		// Check if this is a snapshot backup
		if IsSnapshotBackup(backupName, *sentinel) {
			tracelog.InfoLogger.Printf("Detected snapshot backup %s, executing delete command", backupName)
			err := ExecuteSnapshotDeleteCommand(backupName)
			if err != nil {
				// Log error but don't fail the deletion process
				tracelog.WarningLogger.Printf("Failed to execute snapshot delete command for %s: %v", 
					backupName, err)
			}
		}
	}
}

