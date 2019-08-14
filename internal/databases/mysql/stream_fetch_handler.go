package mysql

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path"
	"path/filepath"
	"sort"
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
	dstFolder, fetchedBinlogs, err := getNecessaryBinlog(folder, backupUploadTime)
	if err != nil {
		return err
	}
	return createIndexFile(dstFolder, fetchedBinlogs)
}

func createIndexFile(dstFolder string, fetchedBinlogs []storage.Object) error {
	indexFile, err := os.Create(filepath.Join(dstFolder, "binlogs_order"))
	if err != nil {
		return err
	}

	for _, object := range fetchedBinlogs {
		_, err = indexFile.WriteString(utility.TrimFileExtension(object.GetName()) + "\n")
		if err != nil {
			return err
		}
	}
	return indexFile.Close()
}

func getNecessaryBinlog(folder storage.Folder, backupStartUploadTime time.Time) (string, []storage.Object, error) {
	binlogFolder := folder.GetSubFolder(BinlogPath)

	endTS, dstFolder, err := GetBinlogConfigs()
	if err != nil {
		return "", nil, err
	}

	objects, _, err := binlogFolder.ListFolder()
	if err != nil {
		return "", nil, err
	}

	var fetchedLogs []storage.Object

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].GetLastModified().Before(objects[j].GetLastModified())
	})

	for _, object := range objects {
		tracelog.InfoLogger.Println("Consider binlog ", object.GetName(), object.GetLastModified().Format(time.RFC3339))
		binlogName := ExtractBinlogName(object, folder)

		if BinlogShouldBeFetched(backupStartUploadTime, object) {
			fileName := path.Join(dstFolder, binlogName)
			tracelog.InfoLogger.Println("Download", binlogName, "to", fileName)

			if err := internal.DownloadWALFileTo(binlogFolder, binlogName, fileName); err != nil {
				return "", nil, err
			}

			binlogFirstEventTimestamp, err := parseFromBinlog(fileName)

			if err != nil {
				return "", nil, err
			}

			if BinlogIsTooOld(binlogFirstEventTimestamp, endTS) {
				if err = os.Remove(fileName); err != nil {
					return "", nil, err
				}
				return dstFolder, fetchedLogs, nil
			}

			fetchedLogs = append(fetchedLogs, object)
		}
	}

	return dstFolder, fetchedLogs, nil
}

func BinlogShouldBeFetched(backupStartUploadTime time.Time, object storage.Object) bool {
	return backupStartUploadTime.Before(object.GetLastModified()) || backupStartUploadTime.Equal(object.GetLastModified())
}

func BinlogIsTooOld(binlogTimestamp time.Time, endTS *time.Time) bool {
	return endTS != nil && binlogTimestamp.After(*endTS)
}

func GetBinlogConfigs() (endTS *time.Time, dstFolder string, err error) {
	return internal.GetOperationLogsSettings(BinlogEndTsSetting, BinlogDstSetting)
}
func ExtractBinlogName(object storage.Object, folder storage.Folder) string {
	binlogName := object.GetName()
	return strings.TrimSuffix(binlogName, "."+utility.GetFileExtension(binlogName))
}
