package common

import (
	"context"
	"fmt"

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
	return canonicalSentinel(&sentinel, backup.Name)
}

func DownloadSentinelForDeletion(ctx context.Context, folder storage.Folder, name string) (*models.Backup, error) {
	backup, err := internal.GetBackupByName(ctx, name, "", folder)
	if err != nil {
		return nil, err
	}
	var sentinel models.Backup
	doc, err := internal.ReadDeletionMetadata(ctx, backup.Folder, backup.Name+utility.SentinelSuffix, &sentinel)
	if err != nil {
		return nil, err
	}
	if doc.Problem != nil {
		return nil, doc.Problem
	}
	if _, known := doc.Bool("Permanent"); !known {
		return nil, fmt.Errorf("unknown protection in sentinel for %s", backup.Name)
	}
	return canonicalSentinel(&sentinel, backup.Name)
}

func canonicalSentinel(sentinel *models.Backup, name string) (*models.Backup, error) {
	if sentinel.BackupName == "" {
		sentinel.BackupName = name
	} else if sentinel.BackupName != name {
		return nil, fmt.Errorf("sentinel for %s contains conflicting backup name %s", name, sentinel.BackupName)
	}
	if sentinel.BackupType == "" {
		sentinel.BackupType = LogicalBackupType
	}
	return sentinel, nil
}

func GetBackupFolder(ctx context.Context) (backupFolder storage.Folder, err error) {
	st, err := internal.ConfigureStorage(ctx)
	if err != nil {
		return nil, err
	}
	return st.RootFolder().GetSubFolder(utility.BaseBackupPath), err
}
