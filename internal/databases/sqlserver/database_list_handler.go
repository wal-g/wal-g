package sqlserver

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleDatabaseList(ctx context.Context, backupName string) {
	storage, err := internal.ConfigureStorage(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	backup, err := internal.GetBackupByName(ctx, backupName, utility.BaseBackupPath, storage.RootFolder())
	if err != nil {
		tracelog.ErrorLogger.Fatalf("can't find backup %s: %v", backupName, err)
	}
	sentinel := new(SentinelDto)
	err = backup.FetchSentinel(ctx, sentinel)
	tracelog.ErrorLogger.FatalOnError(err)
	for _, name := range sentinel.Databases {
		fmt.Println(name)
	}
}
