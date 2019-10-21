package mysql

import (
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"os"
	"time"
)

type BinlogFetchSettings struct {
	dt time.Time
}

func (settings BinlogFetchSettings) GetEndTS() (*time.Time, error) {
	return &settings.dt, nil
}

func (settings BinlogFetchSettings) GetDestFolderPath() (string, error) {
	return internal.GetLogsDstSettingsFromEnv(BinlogDstSetting)
}

func (settings BinlogFetchSettings) GetLogFolderPath() string {
	return BinlogPath
}

func HandleBinlogFetch(folder storage.Folder, backupName string, dt time.Time) error {
	if !internal.FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	backup, err := internal.GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %+v\n", err)
	backupUploadTime, err := getBackupUploadTime(folder, backup)

	if err != nil {
		return err
	}

	settings := BinlogFetchSettings{dt: dt}
	return FetchLogs(folder, backupUploadTime, settings)
}
