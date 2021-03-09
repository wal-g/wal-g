package fdb

import (
	"context"
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, targetBackupSelector internal.BackupSelector, restoreCmd *exec.Cmd) {
	internal.HandleBackupFetch(folder, targetBackupSelector, internal.GetCommandStreamFetcher(restoreCmd))
}
