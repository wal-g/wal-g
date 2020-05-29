package fdb

import (
	"context"
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd) {
	internal.HandleBackupFetch(folder, backupName, internal.GetCommandStreamFetcher(restoreCmd))
}

