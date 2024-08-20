package mysql

import (
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupFetch(folder storage.Folder,
	targetBackupSelector internal.BackupSelector,
	restoreCmd *exec.Cmd,
	prepareCmd *exec.Cmd,
	useXbtoolExtract bool) {
	backup, err := targetBackupSelector.Select(folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get backup: %v", err)

	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch sentinel: %v", err)

	// we should ba able to read & restore any backup we ever created:
	if sentinel.Tool == WalgXtrabackupTool {
		internal.HandleBackupFetch(folder, targetBackupSelector, GetXtrabackupFetcher(restoreCmd, prepareCmd, useXbtoolExtract))
	} else {
		internal.HandleBackupFetch(folder, targetBackupSelector, internal.GetBackupToCommandFetcher(restoreCmd))
		if prepareCmd != nil {
			err = prepareCmd.Run()
			tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
		}
	}
}
