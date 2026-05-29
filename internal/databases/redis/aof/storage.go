package aof

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func SentinelWithExistenceCheck(ctx context.Context, folder storage.Folder, backupName string) (archive.Backup, error) {
	sentinel, err := archive.SentinelWithExistenceCheck(ctx, folder, backupName)
	if err != nil {
		return archive.Backup{}, err
	}
	if sentinel.Version == "" {
		return archive.Backup{}, fmt.Errorf("expecting sentinel file for aof backup with always filled version: %+v", sentinel)
	}
	return sentinel, nil
}
