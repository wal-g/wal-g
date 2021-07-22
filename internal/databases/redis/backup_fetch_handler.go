package redis

import (
	"context"
	"os/exec"

	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd) error {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return err
	}
	return internal.StreamBackupToCommandStdin(restoreCmd, backup)
}
