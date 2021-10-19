package fdb

import (
	"context"
	"os/exec"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupFetch(ctx context.Context,
	folder storage.Folder,
	targetBackupSelector internal.BackupSelector,
	restoreCmd *exec.Cmd) {
	fetcher := internal.GetCommandStreamFetcher(restoreCmd, internal.DownloadAndDecompressStream)
	internal.HandleBackupFetch(folder, targetBackupSelector, fetcher)
}
