package fdb

import (
	"context"
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupFetch(ctx context.Context,
	folder storage.Folder,
	targetBackupSelector internal.BackupSelector,
	restoreCmd *exec.Cmd) {
	backup, err := internal.SelectBackup(folder, targetBackupSelector)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	err = internal.FetchBackupPartsToStdin(restoreCmd, backup)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
}
