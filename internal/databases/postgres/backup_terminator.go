package postgres

import (
	"fmt"
	"log/slog"
	"os"
	"path"
)

const backupLabelFileName = "backup_label"
const backupLabelDstFileName = "backup_label.old"

type BackupTerminator struct {
	queryRunner       *PgQueryRunner
	removeBackupLabel bool
	pgDataDir         string
}

func NewBackupTerminator(queryRunner *PgQueryRunner, pgVersion int, pgDataDir string) *BackupTerminator {
	// for PostgreSQL version earlier than v9.6, WAL-G uses an exclusive backup,
	// so it is useful to remove the backup label on backup termination
	removeBackupLabel := pgVersion < 90600
	return &BackupTerminator{queryRunner: queryRunner, removeBackupLabel: removeBackupLabel, pgDataDir: pgDataDir}
}

func (t *BackupTerminator) TerminateBackup() {
	_, _, _, err := t.queryRunner.StopBackup()
	if err == nil {
		slog.Info(fmt.Sprintf("Successfully stopped the running backup"))
		return
	}

	slog.Warn(fmt.Sprintf("Failed to stop backup: %v", err))
	// failed to stop backup, try to rename the backup_label file (if required)
	t.renameBackupLabel()
}

func (t *BackupTerminator) renameBackupLabel() {
	if !t.removeBackupLabel {
		return
	}

	backupLabelPath := path.Join(t.pgDataDir, backupLabelFileName)
	backupLabelDstPath := path.Join(t.pgDataDir, backupLabelDstFileName)
	err := os.Rename(backupLabelPath, backupLabelDstPath)
	if err != nil {
		slog.Warn(fmt.Sprintf("Failed to rename the backup label file (%s -> %s): %v", backupLabelPath, backupLabelDstPath, err))
		return
	}
	slog.Info(fmt.Sprintf("Successfully renamed the backup label file (%s -> %s)", backupLabelPath, backupLabelDstPath))
}
