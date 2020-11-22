package mysql

import (
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupFetch(folder storage.Folder, backupName string, restoreCmd *exec.Cmd, prepareCmd *exec.Cmd) {
	internal.HandleBackupFetch(folder, backupName, internal.CommandStreamFetcher(restoreCmd))
	if prepareCmd != nil {
		err := prepareCmd.Run()
		tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
	}
}
