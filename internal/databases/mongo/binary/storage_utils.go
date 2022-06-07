package binary

import (
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func DownloadSentinel(folder storage.Folder, backupName string) (*MongodBackupMeta, error) {
	var sentinel MongodBackupMeta
	err := common.DownloadSentinel(folder, backupName, &sentinel)
	if err != nil {
		return nil, err
	}
	return &sentinel, err
}
