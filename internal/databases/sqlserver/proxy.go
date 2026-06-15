package sqlserver

import (
	"context"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func RunProxy(ctx context.Context, folder storage.Folder) {
	bs, err := blob.NewServer(folder)
	tracelog.ErrorLogger.FatalfOnError("proxy create error: %v", err)
	lock, err := bs.AcquireLock()
	tracelog.ErrorLogger.FatalOnError(err)
	defer func() { tracelog.ErrorLogger.PrintOnError(lock.Close()) }()
	err = bs.Run(ctx)
	tracelog.ErrorLogger.FatalfOnError("proxy run error: %v", err)
}
