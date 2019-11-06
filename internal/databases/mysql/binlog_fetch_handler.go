package mysql

import (
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type BinlogFetchSettings struct {
	dt time.Time
}

func (settings BinlogFetchSettings) GetEndTS() (*time.Time, error) {
	return &settings.dt, nil
}

func (settings BinlogFetchSettings) GetDestFolderPath() (string, error) {
	return internal.GetLogsDstSettings(BinlogDstSetting)
}

func (settings BinlogFetchSettings) GetLogFolderPath() string {
	return BinlogPath
}

type BinlogFetchHandlers struct {
	dstPathFolder string
	endTS         *time.Time
}

func (handlers BinlogFetchHandlers) HandleAbortFetch(logFilePath string) error {
	return os.Remove(logFilePath)
}

func (handlers BinlogFetchHandlers) DownloadLogTo(logFolder storage.Folder, logName string, dstLogFilePath string) error {
	if err := internal.DownloadWALFileTo(logFolder, logName, dstLogFilePath); err != nil {
		tracelog.ErrorLogger.Print(err)
		return err
	}
	return nil
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

func FetchLogs(folder storage.Folder, backupUploadTime time.Time, untilDT string) error {
	dt, err := time.Parse(time.RFC3339, untilDT)
	settings := BinlogFetchSettings{dt: dt}
	endTS, err := settings.GetEndTS()

	dstFolder, err := settings.GetDestFolderPath()
	if err != nil {
		return err
	}

	handlers := BinlogFetchHandlers{dstPathFolder: dstFolder, endTS: endTS,}

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

func HandleBinlogFetch(folder storage.Folder, backupName string, untilDT string, needApply bool) error {
	if !internal.FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	backup, err := internal.GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %+v\n", err)
	backupUploadTime, err := getBackupUploadTime(folder, backup)

	if err != nil {
		return err
	}
	return FetchLogs(folder, backupUploadTime, untilDT)
}