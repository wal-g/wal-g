package mysql

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// FIXME: why do we use BackupObject instead of internal.GenericMetadata ?
func newBackupObject(incrementBase, incrementFrom string, isFullBackup bool, object storage.Object) BackupObject {
	return BackupObject{
		BackupObject:      internal.NewDefaultBackupObject(object),
		isFullBackup:      isFullBackup,
		baseBackupName:    incrementBase,
		incrementFromName: incrementFrom,
	}
}

type BackupObject struct {
	internal.BackupObject
	isFullBackup      bool
	baseBackupName    string
	incrementFromName string
}

func (o BackupObject) IsFullBackup() bool {
	return o.isFullBackup
}

func (o BackupObject) GetBaseBackupName() string {
	return o.baseBackupName
}

func (o BackupObject) GetIncrementFromName() string {
	return o.incrementFromName
}

func MakeMySQLBackupObjects(ctx context.Context, folder storage.Folder, sentinelObjects []storage.Object) ([]internal.BackupObject, error) {
	backupObjects := make([]internal.BackupObject, 0, len(sentinelObjects))
	for _, object := range sentinelObjects {
		incrementBase, incrementFrom, isFullBackup, err := getIncrementInfo(ctx, folder.GetSubFolder(utility.BaseBackupPath), object)
		if err != nil {
			return nil, err
		}
		mysqlBackup := newBackupObject(incrementBase, incrementFrom, isFullBackup, object)

		backupObjects = append(backupObjects, mysqlBackup)
	}
	return backupObjects, nil
}

func getIncrementInfo(ctx context.Context, folder storage.Folder, object storage.Object) (string, string, bool, error) {
	backup, err := internal.NewBackup(folder, utility.StripRightmostBackupName(object.GetName()))
	if err != nil {
		return "", "", true, err
	}
	var sentinel = StreamSentinelDto{}
	err = backup.FetchSentinel(ctx, &sentinel)
	if err != nil {
		return "", "", true, err
	}
	if err := validateDeletionIncrement(backup.Name, &sentinel); err != nil {
		return "", "", false, err
	}
	doc, readErr := internal.ReadDeletionMetadata(ctx, backup.Folder, backup.Name+utility.SentinelSuffix, nil)
	if readErr != nil {
		return "", "", false, readErr
	}
	if _, known := doc.Bool("IsPermanent"); doc.Problem != nil || !known {
		return "", "", false, fmt.Errorf("unknown protection metadata for %s", backup.Name)
	}
	if !sentinel.IsIncremental {
		return "", "", true, nil
	}

	return *sentinel.IncrementFullName, *sentinel.IncrementFrom, false, nil
}

func validateDeletionIncrement(name string, sentinel *StreamSentinelDto) error {
	count, fromLSN := sentinel.IncrementCount, sentinel.IncrementFromLSN
	if !sentinel.IsIncremental {
		if count != nil && *count == 0 {
			count = nil
		}
		if fromLSN != nil && *fromLSN == 0 {
			fromLSN = nil
		}
	}
	if err := internal.ValidateDeletionIncrement(name, sentinel.IncrementFrom, sentinel.IncrementFullName,
		count, fromLSN != nil, true); err != nil {
		return err
	}
	if sentinel.IsIncremental != (sentinel.IncrementFrom != nil) {
		return fmt.Errorf("inconsistent IsIncremental for %s", name)
	}
	return nil
}
