package postgres

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/jackc/pgx"
	"github.com/wal-g/tracelog"
)

const backupLabelFileName = "backup_label"
const backupLabelDstFileName = "backup_label.old"
const stopBackupTimeout = 1 * time.Minute

type BackupTerminator struct {
	conn              *pgx.Conn
	removeBackupLabel bool
	pgDataDir         string
}

func NewBackupTerminator(conn *pgx.Conn, pgVersion int, pgDataDir string) *BackupTerminator {
	// for PostgreSQL version earlier than v9.6, WAL-G uses an exclusive backup,
	// so it is useful to remove the backup label on backup termination
	removeBackupLabel := pgVersion < 90600
	return &BackupTerminator{conn: conn, removeBackupLabel: removeBackupLabel, pgDataDir: pgDataDir}
}

func (t *BackupTerminator) TerminateBackup() {
	stopBackupErrCh := make(chan error, 1)

	go func() {
		err := t.tryStopPgBackup()
		stopBackupErrCh <- err
	}()

	var err error
	select {
	case stopErr := <-stopBackupErrCh:
		if stopErr == nil {
			tracelog.InfoLogger.Printf("Successfully stopped the running backup")
			return
		}

		err = stopErr

	case <-time.After(stopBackupTimeout):
		err = fmt.Errorf("run out of time (%s)", stopBackupTimeout.String())
	}

	tracelog.WarningLogger.Printf("Failed to stop backup: %v", err)
	// failed to stop backup, try to rename the backup_label file (if required)
	t.renameBackupLabel()
}

func (t *BackupTerminator) tryStopPgBackup() error {
	queryRunner, err := NewPgQueryRunner(t.conn)
	if err != nil {
		return fmt.Errorf("failed to build query runner: %w", err)
	}

	_, _, _, err = queryRunner.stopBackup()
	if err != nil {
		return fmt.Errorf("failed to stop backup: %w", err)
	}
	return nil
}

func (t *BackupTerminator) renameBackupLabel() {
	if !t.removeBackupLabel {
		return
	}

	backupLabelPath := path.Join(t.pgDataDir, backupLabelFileName)
	backupLabelDstPath := path.Join(t.pgDataDir, backupLabelDstFileName)
	err := os.Rename(backupLabelPath, backupLabelDstPath)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to rename the backup label file (%s -> %s): %v", backupLabelPath, backupLabelDstPath, err)
		return
	}
	tracelog.InfoLogger.Printf("Successfully renamed the backup label file (%s -> %s)", backupLabelPath, backupLabelDstPath)
}
