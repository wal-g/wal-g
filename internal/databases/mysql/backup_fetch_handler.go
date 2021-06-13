package mysql

import (
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupFetch(folder storage.Folder,
	targetBackupSelector internal.BackupSelector,
	restoreCmd *exec.Cmd,
	prepareCmd *exec.Cmd) {

	backup := internal.GetBackup(folder, targetBackupSelector)
	var sentinel StreamSentinelDto
	err := backup.FetchSentinel(&sentinel)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("Fail to fetch backup sentinel: %v", err)
	}
	if sentinel.FileNames == nil || len(sentinel.FileNames) == 0 {
		internal.GetCommandStreamFetcher(restoreCmd)(folder, backup)
	} else {
		internal.GetCommandStreamFetcherParts(restoreCmd)(folder, backup, sentinel.FileNames)
	}

	if prepareCmd != nil {
		err := prepareCmd.Run()
		tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
	}
}
