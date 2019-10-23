package mysql

import (
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"os"
	"time"
)

func HandleBinlogFetch(folder storage.Folder, backupName string, untilDT time.Time, needApply bool) error {
	if !internal.FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	backup, err := internal.GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %+v\n", err)
	backupUploadTime, err := getBackupUploadTime(folder, backup)

	if err != nil {
		return err
	}
	return FetchLogs(folder, backupUploadTime, untilDT, needApply)
}
