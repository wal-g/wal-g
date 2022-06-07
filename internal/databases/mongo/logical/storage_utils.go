package logical

import (
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func DownloadSentinel(folder storage.Folder, backupName string) (*models.Backup, error) {
	var sentinel models.Backup
	err := common.DownloadSentinel(folder, backupName, &sentinel)
	if err != nil {
		return nil, err
	}
	if sentinel.BackupName == "" {
		sentinel.BackupName = backupName
	}
	return &sentinel, err
}
