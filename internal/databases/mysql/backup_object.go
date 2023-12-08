package mysql

import (
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

func MakeMySQLBackupObjects(folder storage.Folder, sentinelObjects []storage.Object) ([]internal.BackupObject, error) {
	backupObjects := make([]internal.BackupObject, 0, len(sentinelObjects))
	for _, object := range sentinelObjects {
		incrementBase, incrementFrom, isFullBackup, err := getIncrementInfo(folder.GetSubFolder(utility.BaseBackupPath), object)
		if err != nil {
			return nil, err
		}
		mysqlBackup := newBackupObject(incrementBase, incrementFrom, isFullBackup, object)

		backupObjects = append(backupObjects, mysqlBackup)
	}
	return backupObjects, nil
}

func getIncrementInfo(folder storage.Folder, object storage.Object) (string, string, bool, error) {
	backup, err := internal.NewBackup(folder, utility.StripRightmostBackupName(object.GetName()))
	if err != nil {
		return "", "", true, err
	}
	var sentinel = StreamSentinelDto{}
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		return "", "", true, err
	}
	if !sentinel.IsIncremental {
		return "", "", true, nil
	}

	return *sentinel.IncrementFullName, *sentinel.IncrementFrom, false, nil
}
