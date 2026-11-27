package redis

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func PlanRecoveryDeletion(ctx context.Context, root storage.Folder, destination string,
	names []string, logs bool) (*internal.DeletionPlan, error) {
	load := func(ctx context.Context, root storage.Folder, name string) (internal.DeletionBackup, error) {
		var sentinel archive.Backup
		doc, err := internal.ReadDeletionMetadata(ctx, root.GetSubFolder(utility.BaseBackupPath), name+utility.SentinelSuffix, &sentinel)
		b := internal.DeletionBackup{DependenciesKnown: true, Problem: doc.Problem}
		b.Permanent, b.ProtectionKnown = doc.Bool("Permanent")
		if sentinel.BackupName != "" && sentinel.BackupName != name {
			b.Problem = fmt.Errorf("conflicting backup name %s", sentinel.BackupName)
		}
		return b, err
	}
	plan, err := internal.NewDeletionPlan(ctx, root, destination, names, false, nil, load)
	if err != nil {
		return plan, err
	}
	if logs {
		tracelog.InfoLogger.Println("Redis has no external log archive to clean; attached AOF/TS files are backup payload")
	}
	return plan, plan.PrepareTargets(ctx, nil)
}
