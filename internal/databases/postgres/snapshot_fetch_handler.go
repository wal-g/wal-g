package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Note: fmt is still needed for setupRecoveryConfig

// SnapshotFetchHandler handles preparing snapshot backups for recovery
type SnapshotFetchHandler struct {
	BackupName      string
	TargetDirectory string
	Folder          storage.Folder
}

// NewSnapshotFetchHandler creates a new SnapshotFetchHandler
func NewSnapshotFetchHandler(backupName string, targetDirectory string, folder storage.Folder) *SnapshotFetchHandler {
	return &SnapshotFetchHandler{
		BackupName:      backupName,
		TargetDirectory: targetDirectory,
		Folder:          folder,
	}
}

// HandleSnapshotFetch prepares a snapshot backup for recovery by creating backup_label
func (sfh *SnapshotFetchHandler) HandleSnapshotFetch(ctx context.Context) error {
	tracelog.InfoLogger.Printf("Preparing snapshot backup %s for recovery", sfh.BackupName)

	// Fetch backup metadata
	backup, err := NewBackupInStorage(sfh.Folder.GetSubFolder(utility.BaseBackupPath), sfh.BackupName, "")
	if err != nil {
		return errors.Wrapf(err, "failed to load backup %s", sfh.BackupName)
	}

	// Load the sentinel from storage
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return errors.Wrapf(err, "failed to fetch sentinel for backup %s", sfh.BackupName)
	}

	// Verify this is a snapshot backup
	if !IsSnapshotBackup(sfh.BackupName, sentinel) {
		return errors.Errorf("backup %s is not a snapshot backup", sfh.BackupName)
	}

	tracelog.InfoLogger.Printf("Snapshot backup found: Start LSN %s, Finish LSN %s",
		sentinel.BackupStartLSN.String(), sentinel.BackupFinishLSN.String())

	// Use the exact backup_label content from PostgreSQL
	// NEVER reconstruct these files - use exactly what pg_stop_backup() returned
	if sentinel.BackupLabel == nil || *sentinel.BackupLabel == "" {
		return errors.New("backup_label content is missing from snapshot sentinel - cannot restore")
	}

	// Write backup_label to target directory
	backupLabelPath := filepath.Join(sfh.TargetDirectory, BackupLabelFilename)
	err = os.WriteFile(backupLabelPath, []byte(*sentinel.BackupLabel), 0600)
	if err != nil {
		return errors.Wrapf(err, "failed to write %s", backupLabelPath)
	}
	tracelog.InfoLogger.Printf("Created %s", backupLabelPath)

	// Write tablespace_map if it was provided by PostgreSQL
	if sentinel.TablespaceMap != nil && *sentinel.TablespaceMap != "" {
		tablespaceMapPath := filepath.Join(sfh.TargetDirectory, TablespaceMapFilename)
		err = os.WriteFile(tablespaceMapPath, []byte(*sentinel.TablespaceMap), 0600)
		if err != nil {
			return errors.Wrapf(err, "failed to write %s", tablespaceMapPath)
		}
		tracelog.InfoLogger.Printf("Created %s", tablespaceMapPath)
	}

	tracelog.InfoLogger.Printf("Snapshot backup %s is ready for recovery", sfh.BackupName)
	tracelog.InfoLogger.Printf("You can now start PostgreSQL to begin recovery from this snapshot")

	return nil
}

// HandleSnapshotFetchWithRecovery prepares snapshot and sets up recovery configuration
func (sfh *SnapshotFetchHandler) HandleSnapshotFetchWithRecovery(ctx context.Context, restoreCommand string, recoveryTarget string) error {
	// First, prepare the backup_label
	err := sfh.HandleSnapshotFetch(ctx)
	if err != nil {
		return err
	}

	// Now set up recovery configuration
	return sfh.setupRecoveryConfig(restoreCommand, recoveryTarget)
}

// setupRecoveryConfig creates recovery.signal and configures restore_command
func (sfh *SnapshotFetchHandler) setupRecoveryConfig(restoreCommand string, recoveryTarget string) error {
	// Check PostgreSQL version to determine recovery method
	pgVersionPath := filepath.Join(sfh.TargetDirectory, "PG_VERSION")
	versionData, err := os.ReadFile(pgVersionPath)
	if err != nil {
		return errors.Wrap(err, "failed to read PG_VERSION")
	}

	var pgMajorVersion int
	_, err = fmt.Sscanf(string(versionData), "%d", &pgMajorVersion)
	if err != nil {
		return errors.Wrap(err, "failed to parse PG_VERSION")
	}

	if pgMajorVersion >= 12 {
		// PostgreSQL 12+: Use recovery.signal and postgresql.auto.conf
		recoverySignalPath := filepath.Join(sfh.TargetDirectory, "recovery.signal")
		err = os.WriteFile(recoverySignalPath, []byte("# Recovery signal file\n"), 0600)
		if err != nil {
			return errors.Wrapf(err, "failed to create recovery.signal")
		}
		tracelog.InfoLogger.Printf("Created recovery.signal")

		// Append to postgresql.auto.conf
		autoConfPath := filepath.Join(sfh.TargetDirectory, "postgresql.auto.conf")
		autoConf := fmt.Sprintf("\n# WAL-G snapshot recovery configuration\nrestore_command = '%s'\n", restoreCommand)
		if recoveryTarget != "" {
			autoConf += fmt.Sprintf("recovery_target_time = '%s'\n", recoveryTarget)
		}

		f, err := os.OpenFile(autoConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return errors.Wrapf(err, "failed to open postgresql.auto.conf")
		}
		defer f.Close()

		_, err = f.WriteString(autoConf)
		if err != nil {
			return errors.Wrap(err, "failed to write recovery config")
		}
		tracelog.InfoLogger.Printf("Updated postgresql.auto.conf with recovery settings")
	} else {
		// PostgreSQL 11 and earlier: Use recovery.conf
		recoveryConfPath := filepath.Join(sfh.TargetDirectory, "recovery.conf")
		recoveryConf := fmt.Sprintf("restore_command = '%s'\n", restoreCommand)
		if recoveryTarget != "" {
			recoveryConf += fmt.Sprintf("recovery_target_time = '%s'\n", recoveryTarget)
		}

		err = os.WriteFile(recoveryConfPath, []byte(recoveryConf), 0600)
		if err != nil {
			return errors.Wrapf(err, "failed to create recovery.conf")
		}
		tracelog.InfoLogger.Printf("Created recovery.conf")
	}

	return nil
}
