package etcd

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func NewEtcdDeleteHandler(folder storage.Folder) (*internal.DeleteHandler, error) {
	backups, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupObjects := make([]internal.BackupObject, 0, len(backups))
	for _, object := range backups {
		backupObjects = append(backupObjects, internal.NewDefaultBackupObject(object))
	}

	return internal.NewDeleteHandler(folder, backupObjects, makeLessFunc()), nil
}

func makeLessFunc() func(object1, object2 storage.Object) bool {
	return func(object1, object2 storage.Object) bool {
		time1, ok1 := utility.TryFetchTimeRFC3999(object1.GetName())
		time2, ok2 := utility.TryFetchTimeRFC3999(object2.GetName())
		if !ok1 || !ok2 {
			return object2.GetLastModified().After(object1.GetLastModified())
		}
		return time1 < time2
	}
}
