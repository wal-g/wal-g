package internal

import (
	"fmt"
	"os"
	"path"
	"sort"
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

func GetOperationLogsCoveringInterval(folder storage.Folder, start time.Time, end *time.Time) ([]storage.Object, error) {
	oplogFiles, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}

	sort.Slice(oplogFiles, func(i, j int) bool {
		return oplogFiles[i].GetLastModified().After(oplogFiles[j].GetLastModified())
	})

	var logsToFetch []storage.Object

	for _, oplogFile := range oplogFiles {
		if oplogFile.GetLastModified().After(start) {
			logsToFetch = append(logsToFetch, oplogFile)
			if end != nil && oplogFile.GetLastModified().After(*end) {
				break
			}
		}
	}
	return logsToFetch, err
}

func DownloadOplogFiles(oplogFiles []storage.Object, oplogFolder storage.Folder, oplogDstFolder string, logFileName string) error {
	for _, oplogFile := range oplogFiles {
		oplogName := utility.TrimFileExtension(oplogFile.GetName())
		oplogFileSubFolder := path.Join(oplogDstFolder, oplogName)
		_, err := NewDiskDataFolder(oplogFileSubFolder)
		if err != nil {
			return err
		}
		oplogFilePath := path.Join(oplogFileSubFolder, logFileName)
		err = DownloadWALFileTo(oplogFolder, oplogName, oplogFilePath)
		if err != nil {
			return err
		}
		tracelog.InfoLogger.Println("Operation log file " + oplogFile.GetName() + " fetched to " + oplogFilePath)
	}

	return nil
}
