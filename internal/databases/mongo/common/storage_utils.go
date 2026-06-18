package common

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const LogicalBackupType = "logical"
const BinaryBackupType = "binary"

func DownloadMetadata(ctx context.Context, folder storage.Folder, backupName string) (*models.BackupRoutesInfo, error) {
	var metadata models.BackupRoutesInfo
	backup, err := internal.GetBackupByName(ctx, backupName, "", folder)
	if err != nil {
		return nil, err
	}
	if err := backup.FetchMetadata(ctx, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func DownloadSentinel(ctx context.Context, folder storage.Folder, backupName string) (*models.Backup, error) {
	var sentinel models.Backup
	backup, err := internal.GetBackupByName(ctx, backupName, "", folder)
	if err != nil {
		return nil, err
	}
	if err := backup.FetchSentinel(ctx, &sentinel); err != nil {
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

func GetBackupFolder(ctx context.Context) (backupFolder storage.Folder, err error) {
	st, err := internal.ConfigureStorage(ctx)
	if err != nil {
		return nil, err
	}
	return st.RootFolder().GetSubFolder(utility.BaseBackupPath), err
}
