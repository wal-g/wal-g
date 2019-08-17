package mysql

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type BinlogFetchSettings struct{}

func (settings BinlogFetchSettings) GetEndTsEnv() string {
	return BinlogEndTsSetting
}

func (settings BinlogFetchSettings) GetDstEnv() string {
	return BinlogDstSetting
}

func (settings BinlogFetchSettings) GetLogFolderPath() string {
	return BinlogPath
}

type BinlogFetchParams struct {
	folder  storage.Folder
	StartTs time.Time
}

func (params BinlogFetchParams) GetStorageFolder() storage.Folder {
	return params.folder
}

func (params BinlogFetchParams) GetStartTs() time.Time {
	return params.StartTs
}

type BinlogFetchHandlers struct {
	dstFolder string
	endTs     *time.Time
}

func (handlers BinlogFetchHandlers) GetLogFilePath(pathToLog string) (string, error) {
	return path.Join(handlers.dstFolder, pathToLog), nil
}

func (handlers BinlogFetchHandlers) CheckUploadedLog(pathToLog string) (bool, error) {
	return filterBinlogByHeaderTimestamp(pathToLog, handlers.endTs)
}

func FetchLogs(folder storage.Folder, backup *internal.Backup) error {
	var streamSentinel StreamSentinelDto
	settings := BinlogFetchSettings{}

	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return err
	}
	binlogs, _, err := folder.GetSubFolder(BinlogPath).ListFolder()
	if err != nil {
		return err
	}
	var backupUploadTime time.Time
	for _, binlog := range binlogs {
		if strings.HasPrefix(binlog.GetName(), streamSentinel.BinLogStart) {
			backupUploadTime = binlog.GetLastModified()
		}
	}

	endTS, dstFolder, err := internal.GetOperationLogsSettings(settings)

	if err != nil {
		return err
	}

	params := BinlogFetchParams{folder: folder, StartTs: backupUploadTime}
	handlers := BinlogFetchHandlers{dstFolder: dstFolder, endTs: endTS}

	fetchedBinlogs, err := internal.FetchLogs(params, settings, handlers)

	return createIndexFile(dstFolder, fetchedBinlogs)
}

func filterBinlogByHeaderTimestamp(logFilePath string, endTS *time.Time) (bool, error) {
	timestamp, err := parseFromBinlog(logFilePath)
	if err != nil {
		return false, err
	}
	return binlogIsTooOld(timestamp, endTS), nil
}

func binlogIsTooOld(binlogTimestamp time.Time, endTS *time.Time) bool {
	return endTS != nil && binlogTimestamp.After(*endTS)
}

func createIndexFile(dstFolder string, fetchedBinlogs []storage.Object) error {
	indexFile, err := os.Create(filepath.Join(dstFolder, "binlogs_order"))
	if err != nil {
		return err
	}

	for _, binlogName := range fetchedBinlogs {
		_, err = indexFile.WriteString(utility.TrimFileExtension(binlogName.GetName()) + "\n")
		if err != nil {
			return err
		}
	}
	return indexFile.Close()
}
