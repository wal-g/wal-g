package mysql

import (
	"os/exec"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupFetch(folder storage.Folder,
	targetBackupSelector internal.BackupSelector,
	restoreCmd *exec.Cmd,
	prepareCmd *exec.Cmd,
	useXbtoolExtract bool,
	inplace bool,
) {
	backup, err := targetBackupSelector.Select(folder)
	logging.FatalfOnError("Failed to get backup: %v", err)

	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	logging.FatalfOnError("Failed to fetch sentinel: %v", err)

	// we should ba able to read & restore any backup we ever created:
	if sentinel.Tool == WalgXtrabackupTool {
		internal.HandleBackupFetch(folder, targetBackupSelector, GetXtrabackupFetcher(restoreCmd, prepareCmd, useXbtoolExtract, inplace))
	} else {
		internal.HandleBackupFetch(folder, targetBackupSelector, internal.GetBackupToCommandFetcher(restoreCmd))
		if prepareCmd != nil {
			err = prepareCmd.Run()
			logging.FatalfOnError("failed to prepare fetched backup: %v", err)
		}
	}
}
