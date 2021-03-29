package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"time"
)

func NewDefaultBackupObject(object storage.Object) BackupObject {
	return DefaultBackupObject{object}
}

type DefaultBackupObject struct {
	storage.Object
}

func (o DefaultBackupObject) GetBackupName() string {
	return utility.StripBackupName(o.GetName())
}

func (o DefaultBackupObject) GetBaseBackupName() string {
	return o.GetBackupName()
}

func (o DefaultBackupObject) IsFullBackup() bool {
	return true
}

func (o DefaultBackupObject) GetBackupTime() time.Time {
	return o.Object.GetLastModified()
}
