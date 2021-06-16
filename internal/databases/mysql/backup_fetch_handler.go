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

	backup, err := internal.SelectBackup(folder, targetBackupSelector)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("Fail to fetch backup sentinel: %v", err)
	}
	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		tracelog.ErrorLogger.FatalfOnError("Fail to fetch backup sentinel: %v", err)
	}
	if len(sentinel.FileNames) == 0 {
		err = internal.FetchBackupPartsToStdin(restoreCmd, backup)
	} else {
		err = internal.FetchFullBackupToStdin(restoreCmd, backup, sentinel.FileNames)
	}
	tracelog.ErrorLogger.FatalfOnError("Fail to fetch backup sentinel: %v", err)

	if prepareCmd != nil {
		err := prepareCmd.Run()
		tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
	}
}
