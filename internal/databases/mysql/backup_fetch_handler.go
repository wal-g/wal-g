package mysql

import (
	"os"
	"os/exec"
	"path"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupFetch(folder storage.Folder, backupName string, restoreCmd *exec.Cmd, prepareCmd *exec.Cmd) {
	var err error
	if isXtrabackupCmd(restoreCmd) {
		tempDeltaDir := path.Join("/tmp", "delta")
		if _, err = os.Stat(tempDeltaDir); os.IsNotExist(err) {
			err = os.MkdirAll(tempDeltaDir, 0755)
		}
		tracelog.ErrorLogger.FatalOnError(err)

		internal.HandleBackupFetch(folder, backupName, internal.GetMysqlFetcher(restoreCmd, prepareCmd, tempDeltaDir))
		err := os.Remove(tempDeltaDir)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to delete temp dir: %v", err)
		}
		return
	}

	internal.HandleBackupFetch(folder, backupName, internal.GetCommandStreamFetcher(restoreCmd))
	if prepareCmd != nil {
		err = prepareCmd.Run()
		tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
	}
}
