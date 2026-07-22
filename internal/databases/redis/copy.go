package redis

import (
	"context"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func BuildCopyPlan(ctx context.Context, from, to storage.Folder, backupName string) (*copy.Plan, error) {
	plan, err := copy.NewPlan(ctx, from, to)
	if err != nil {
		return nil, err
	}
	names, err := plan.ResolveBackupNames(ctx, backupName)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := plan.AddBackup(name, name); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func HandleCopy(ctx context.Context, fromConfigFile, toConfigFile, backupName string) {
	from, err := internal.StorageFromConfig(ctx, fromConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	to, err := internal.StorageFromConfig(ctx, toConfigFile)
	tracelog.ErrorLogger.FatalOnError(err)
	plan, err := BuildCopyPlan(ctx, from.RootFolder(), to.RootFolder(), backupName)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.ErrorLogger.FatalOnError(copy.ExecuteRaw(ctx, plan))
}
