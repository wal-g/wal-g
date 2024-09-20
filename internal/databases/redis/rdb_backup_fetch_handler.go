package redis

import (
	"context"
	"os/exec"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd) error {
	backup, err := archive.SentinelWithExistenceCheck(folder, backupName)
	if err != nil {
		return err
	}

	return internal.StreamBackupToCommandStdin(restoreCmd, backup.ToInternal(folder))
}
