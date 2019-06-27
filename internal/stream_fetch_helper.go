package internal

import (
	"fmt"
	"os"
	"time"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// GetOperationLogsSettings ... TODO
func GetOperationLogsSettings(OperationLogEndTsSetting string, operationLogsDstSetting string) (endTS *time.Time, dstFolder string, err error) {
	endTSStr, ok := GetSetting(OperationLogEndTsSetting)
	if ok {
		t, err := time.Parse(time.RFC3339, endTSStr)
		if err != nil {
			return nil, "", err
		}
		endTS = &t
	}
	dstFolder, ok = GetSetting(operationLogsDstSetting)
	if !ok {
		return endTS, dstFolder, NewUnsetRequiredSettingError(operationLogsDstSetting)
	}
	return endTS, dstFolder, nil
}

// HandleStreamFetch ... TODO
func HandleStreamFetch(backupName string, folder storage.Folder,
	fetchBackup func(storage.Folder, *Backup) error) {
	backup, err := GetBackupByName(backupName, folder)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Unable to get backup %+v\n", err)
	}
	if !FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	err = fetchBackup(folder, backup)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
func DownloadAndDecompressStream(folder storage.Folder, backup *Backup) error {
	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := TryDownloadWALFile(backup.BaseBackupFolder, getStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = DecompressWALFile(&EmptyWriteIgnorer{WriteCloser: os.Stdout}, archiveReader, decompressor)
		if err != nil {
			return err
		}
		utility.LoggedClose(os.Stdout, "")
		return nil
	}
	return NewArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
}
