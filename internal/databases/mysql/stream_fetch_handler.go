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

func (settings BinlogFetchSettings) GetFilePath(dstFolder string, logName string) (string, error) {
	return path.Join(dstFolder, logName), nil
}

func FetchLogs(folder storage.Folder, backup *internal.Backup) error {
	var streamSentinel StreamSentinelDto
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

	logsToFetch, err := internal.GetLogsCoveringInterval(folder, backupUploadTime, nil)
	if err != nil {
		return err
	}
	endTS, logDstFolder, err := internal.GetOperationLogsSettings(BinlogFetchSettings{}.GetEndTsEnv(), BinlogFetchSettings{}.GetDstEnv())

	if err != nil {
		return err
	}

	fetchedBinlogs, err := filterBinlogByHeaderTimestamp(folder, logsToFetch, logDstFolder, endTS)

	if err != nil {
		return err
	}

	return createIndexFile(logDstFolder, fetchedBinlogs)
}

func filterBinlogByHeaderTimestamp(folder storage.Folder, logsToFetch []storage.Object, logDstFolder string, endTS *time.Time) ([]string, error) {
	var actuallyFetched []string

	for _, binlog := range logsToFetch {
		binlogName := utility.TrimFileExtension(binlog.GetName())
		logFilePath := path.Join(logDstFolder, binlogName)

		err := internal.DownloadWALFileTo(folder, binlogName, logFilePath)
		if err != nil {
			return actuallyFetched, err
		}

		timestamp, err := parseFromBinlog(logFilePath)
		if err != nil {
			return nil, err
		}

		if BinlogIsTooOld(timestamp, endTS) {
			if err := os.Remove(logFilePath); err != nil {
				return nil, err
			}
			return actuallyFetched, err
		}
		actuallyFetched = append(actuallyFetched, binlogName)
	}
	return actuallyFetched, nil
}

func BinlogIsTooOld(binlogTimestamp time.Time, endTS *time.Time) bool {
	return endTS != nil && binlogTimestamp.After(*endTS)
}

func createIndexFile(dstFolder string, fetchedBinlogs []string) error {
	indexFile, err := os.Create(filepath.Join(dstFolder, "binlogs_order"))
	if err != nil {
		return err
	}

	for _, binlogName := range fetchedBinlogs {
		_, err = indexFile.WriteString(utility.TrimFileExtension(binlogName) + "\n")
		if err != nil {
			return err
		}
	}
	return indexFile.Close()
}
