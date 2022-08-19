package common

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const LogicalBackupType = "logical"
const BinaryBackupType = "binary"

func DownloadSentinel(folder storage.Folder, backupName string) (*models.Backup, error) {
	var sentinel models.Backup
	backup := internal.NewBackup(folder, backupName)
	if err := backup.FetchSentinel(&sentinel); err != nil {
		return nil, err
	}
	if sentinel.BackupName == "" {
		sentinel.BackupName = backupName
	}
	if sentinel.BackupType == "" {
		sentinel.BackupType = LogicalBackupType
	}
	return &sentinel, nil
}

func GetBackupFolder() (backupFolder storage.Folder, err error) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, err
	}
	return folder.GetSubFolder(utility.BaseBackupPath), err
}
