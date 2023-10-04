package mysql

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func makeLessFunc(folder storage.Folder) func(object1, object2 storage.Object) bool {
	return func(object1, object2 storage.Object) bool {
		time1, ok := utility.TryFetchTimeRFC3999(object1.GetName())
		if !ok {
			time1 = object1.GetLastModified().Format(utility.BackupTimeFormat)
		}
		time2, ok := utility.TryFetchTimeRFC3999(object2.GetName())
		if !ok {
			time2 = object2.GetLastModified().Format(utility.BackupTimeFormat)
		}
		return time1 < time2
	}
}

func NewDeleteHandler(folder storage.Folder) (*internal.DeleteHandler, error) {
	backupSentinels, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupObjects, err := MakeMySQLBackupObjects(folder, backupSentinels)
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups := internal.GetPermanentBackups(folder.GetSubFolder(utility.BaseBackupPath), NewGenericMetaFetcher())

	return internal.NewDeleteHandler(
		folder,
		backupObjects,
		makeLessFunc(folder),
		internal.IsPermanentFunc(func(object storage.Object) bool {
			return internal.IsPermanent(object.GetName(), permanentBackups, internal.StreamBackupNameLength)
		}),
	), nil
}
