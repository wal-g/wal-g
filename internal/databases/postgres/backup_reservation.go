package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Outside basebackups_005: normal backup deletion/garbage collection must not
// release a name while an uploader might still be writing to it.
const backupReservationsPath = "backup_reservations"

func reserveBackupName(ctx context.Context, root storage.Folder, backupsPath, name string) error {
	if err := multistorage.EnsureSingleStorageIsUsed(root); err != nil {
		return err
	}
	if !storage.SupportsCreateObject(root) {
		return nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	content, err := json.Marshal(struct {
		Host      string    `json:"host"`
		PID       int       `json:"pid"`
		CreatedAt time.Time `json:"created_at"`
	}{hostname, os.Getpid(), time.Now().UTC()})
	if err != nil {
		return err
	}
	marker := path.Join(backupReservationsPath, backupsPath, name+".json")
	if err := storage.CreateObject(ctx, root, marker, content); err != nil {
		return fmt.Errorf("cannot reserve backup name %q (%s); no backup data uploaded: %w", name, marker, err)
	}
	// Keep the reservation on every exit, including failed inspection. Older
	// writers did not reserve names, so check their completed AND partial backups.
	backups := root.GetSubFolder(backupsPath)
	exists, err := backups.Exists(ctx, name+utility.SentinelSuffix)
	if err != nil {
		return fmt.Errorf("check existing backup %q: %w", name, err)
	}
	if exists {
		return fmt.Errorf("backup %q already exists: %w", name, storage.ErrObjectAlreadyExists)
	}
	objects, folders, err := backups.GetSubFolder(name).ListFolder(ctx)
	if err != nil {
		return fmt.Errorf("check existing backup data %q: %w", name, err)
	}
	if len(objects) != 0 || len(folders) != 0 {
		return fmt.Errorf("backup data for %q already exists: %w", name, storage.ErrObjectAlreadyExists)
	}
	return nil
}

// EnableBackupNameReservation is called unconditionally by PostgreSQL's
// backup-push command. The shared handler also serves Greenplum segment backups,
// whose storage layout and cleanup protocol are outside this change.
func (ba *BackupArguments) EnableBackupNameReservation() {
	ba.reserveBackupName = true
}

func (bh *BackupHandler) reserveBackupName(ctx context.Context, root storage.Folder, backupsPath, name string) error {
	if !bh.Arguments.reserveBackupName {
		return nil
	}
	return reserveBackupName(ctx, root, backupsPath, name)
}
