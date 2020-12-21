package mysql

import (
	"time"

	"github.com/wal-g/storages/storage"
)

type BackupObject struct {
	storage.Object
}

func (o BackupObject) IsFullBackup() bool {
	return true
}

func (o BackupObject) GetBackupTime() time.Time {
	return o.Object.GetLastModified()
}
