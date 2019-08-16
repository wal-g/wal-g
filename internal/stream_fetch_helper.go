package internal

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"sort"
	"time"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type LogFetchSettings interface {
	GetEndTsEnv() string
	GetDstEnv() string
	GetLogFolderPath() string
	GetFilePath(string, string) (string, error)
}

// GetOperationLogsSettings reads from the environment variables fetch settings
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

// HandleStreamFetch is invoked to perform wal-g stream-fetch
func HandleStreamFetch(backupName string, folder storage.Folder,
	fetchLogs func(storage.Folder, *Backup) error) {
	if !FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}

	backup, err := GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %+v\n", err)
	logsAreDone := make(chan error)
	go func() {
		logsAreDone <- fetchLogs(folder, backup)
	}()
	err = DownloadAndDecompressStream(backup)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Println("Waiting for logs")
	err = <-logsAreDone
	tracelog.ErrorLogger.FatalOnError(err)
}

// DownloadAndDecompressStream downloads, decompresses and writes stream to stdout
func DownloadAndDecompressStream(backup *Backup) error {
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

// GetLogsCoveringInterval lists the operation logs that cover the interval
func GetLogsCoveringInterval(folder storage.Folder, start time.Time, end *time.Time) ([]storage.Object, error) {
	logFiles, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}

	var logsToFetch []storage.Object

	for _, logFile := range logFiles {
		tracelog.InfoLogger.Println("Consider log file: ", logFile.GetName(), logFile.GetLastModified().Format(time.RFC3339))
		if LogFileShouldBeFetched(start, end, logFile) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	sort.Slice(logsToFetch, func(i, j int) bool {
		return logsToFetch[i].GetLastModified().Before(logsToFetch[j].GetLastModified())
	})
	return logsToFetch, nil
}

// DownloadLogFiles downloads files to specified folder
func DownloadLogFiles(logFiles []storage.Object, logFolder storage.Folder, logDstFolder string, getLogFilePath func(string, string) (string, error)) error {
	for _, logFile := range logFiles {
		logName := utility.TrimFileExtension(logFile.GetName())

		logFilePath, err := getLogFilePath(logDstFolder, logName)
		if err != nil {
			return err
		}

		tracelog.InfoLogger.Printf("Download %v to %v\n", logName, logFilePath)
		err = DownloadWALFileTo(logFolder, logName, logFilePath)
		if err != nil {
			return err
		}
	}

	return nil
}

func FetchLogs(folder storage.Folder, startTime time.Time, settings LogFetchSettings) (logDstFolder string, fetched []storage.Object, err error) {
	endTS, logDstFolder, err := GetOperationLogsSettings(settings.GetEndTsEnv(), settings.GetDstEnv())
	if err != nil {
		return "", nil, err
	}
	logFolder := folder.GetSubFolder(settings.GetLogFolderPath())
	logsToFetch, err := GetLogsCoveringInterval(logFolder, startTime, endTS)
	if err != nil {
		return "", nil, err
	}

	err = DownloadLogFiles(logsToFetch, logFolder, logDstFolder, settings.GetFilePath)
	if err != nil {
		return "", nil, err
	}
	return logDstFolder, logsToFetch, nil
}

func LogFileShouldBeFetched(backupStartUploadTime time.Time, endTS *time.Time, object storage.Object) bool {
	return (backupStartUploadTime.Before(object.GetLastModified()) || backupStartUploadTime.Equal(object.GetLastModified())) &&
		(endTS == nil || (*endTS).After(object.GetLastModified()))
}

// TODO : unit tests
func FetchStreamSentinel(backup *Backup, sentinelDto interface{}) error {
	sentinelDtoData, err := backup.FetchSentinelData()
	if err != nil {
		return errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, sentinelDto)
	return errors.Wrap(err, "failed to unmarshal sentinel")
}
