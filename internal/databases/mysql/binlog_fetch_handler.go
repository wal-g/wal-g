package mysql

import (
	"github.com/tinsane/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type BinlogFetchHandlers struct {
	dstPathFolder string
	endTS         *time.Time
}

func (handlers BinlogFetchHandlers) HandleAbortFetch(logFilePath string) error {
	return os.Remove(logFilePath)
}

func (handlers BinlogFetchHandlers) DownloadLogTo(logFolder storage.Folder, logName string, dstLogFilePath string) error {
	return internal.DownloadWALFileTo(logFolder, logName, dstLogFilePath)
}

func (handlers BinlogFetchHandlers) GetLogFilePath(pathToLog string) (string, error) {
	return path.Join(handlers.dstPathFolder, pathToLog), nil
}

func (handlers BinlogFetchHandlers) ShouldBeAborted(pathToLog string) (bool, error) {
	timestamp, err := parseFromBinlog(pathToLog)
	if err != nil {
		return false, err
	}
	return binlogIsTooOld(timestamp, handlers.endTS), nil
}

func FetchLogs(folder storage.Folder, backupUploadTime time.Time, settings internal.LogFetchSettings) error {
	endTS, err := settings.GetEndTS()
	dstFolder, err := settings.GetDestFolderPath()
	if err != nil {
		return err
	}
	handlers := BinlogFetchHandlers{dstPathFolder: dstFolder, endTS: endTS}
	fetchedBinlogs, err := internal.FetchLogs(folder, backupUploadTime, nil, settings.GetLogFolderPath(), handlers)

	if err != nil {
		return err
	}

	return createIndexFile(dstFolder, fetchedBinlogs)
}

func getBackupUploadTime(folder storage.Folder, backup *internal.Backup) (time.Time, error) {
	var streamSentinel StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return time.Time{}, err
	}

	binlogs, _, err := folder.GetSubFolder(BinlogPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}

	var backupUploadTime time.Time
	for _, binlog := range binlogs {
		if strings.HasPrefix(binlog.GetName(), streamSentinel.BinLogStart) {
			backupUploadTime = binlog.GetLastModified()
		}
	}

	return backupUploadTime, nil
}

func binlogIsTooOld(binlogTimestamp time.Time, endTS *time.Time) bool {
	return endTS != nil && binlogTimestamp.After(*endTS)
}

func createIndexFile(dstFolder string, fetchedBinlogs []storage.Object) error {
	indexFile, err := os.Create(filepath.Join(dstFolder, "binlogs_order"))
	if err != nil {
		return err
	}

	for _, binlogObject := range fetchedBinlogs {
		_, err = indexFile.WriteString(utility.TrimFileExtension(binlogObject.GetName()) + "\n")
		if err != nil {
			return err
		}
	}
	return indexFile.Close()
}
