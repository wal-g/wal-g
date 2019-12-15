package internal

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"sort"
	"time"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
)

type LogFetchSettings interface {
	GetEndTS() (*time.Time, error)
	GetDestFolderPath() (string, error)
	GetLogFolderPath() string
}

type LogFetchHandlers interface {
	GetLogFilePath(pathToLog string) (string, error)
	ShouldBeAborted(pathToLog string) (bool, error)
	DownloadLogTo(logFolder storage.Folder, logName string, dstLogFilePath string) error
	HandleAbortFetch(string) error
}

// TODO : unit tests
func ParseTS(endTSEnvVar string) (endTS *time.Time, err error) {
	endTSStr, ok := GetSetting(endTSEnvVar)
	if ok {
		t, err := time.Parse(time.RFC3339, endTSStr)
		if err != nil {
			return nil, err
		}
		endTS = &t
	}
	return endTS, nil
}

// TODO : unit tests
// GetLogsDstSettings reads from the environment variables fetch settings
func GetLogsDstSettings(operationLogsDstEnvVariable string) (dstFolder string, err error) {
	dstFolder, ok := GetSetting(operationLogsDstEnvVariable)
	if !ok {
		return dstFolder, NewUnsetRequiredSettingError(operationLogsDstEnvVariable)
	}
	return dstFolder, nil
}

// TODO : unit tests
// DownloadAndDecompressStream downloads, decompresses and writes stream to stdout
func downloadAndDecompressStream(backup *Backup, writeCloser io.WriteCloser) error {
	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := TryDownloadWALFile(backup.BaseBackupFolder, getStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = decompressWALFile(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
		if err != nil {
			return err
		}
		utility.LoggedClose(writeCloser, "")
		return nil
	}
	return newArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
}

// TODO : unit tests
// GetLogsCoveringInterval lists the operation logs that cover the interval
func getLogsCoveringInterval(folder storage.Folder, start time.Time, end *time.Time) ([]storage.Object, error) {
	logFiles, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].GetLastModified().Before(logFiles[j].GetLastModified())
	})

	var logsToFetch []storage.Object
	for _, logFile := range logFiles {
		tracelog.InfoLogger.Println("Consider log file: ", logFile.GetName(), logFile.GetLastModified().Format(time.RFC3339))
		if logFileShouldBeFetched(start, end, logFile) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	return logsToFetch, nil
}

// TODO : unit tests
// DownloadLogFiles downloads files to specified folder
func downloadLogFiles(logFiles []storage.Object, logFolder storage.Folder, handlers LogFetchHandlers) ([]storage.Object, error) {
	var fetched []storage.Object
	for _, logFile := range logFiles {
		logName := utility.TrimFileExtension(logFile.GetName())

		logFilePath, err := handlers.GetLogFilePath(logName)
		if err != nil {
			return nil, err
		}

		tracelog.InfoLogger.Printf("Download %v to %v\n", logName, logFilePath)
		err = handlers.DownloadLogTo(logFolder, logName, logFilePath)
		if err != nil {
			return nil, err
		}

		needAbortFetch, err := handlers.ShouldBeAborted(logFilePath)
		if err != nil {
			return nil, err
		}
		if needAbortFetch {
			if err = handlers.HandleAbortFetch(logFilePath); err != nil {
				return nil, err
			}

			return fetched, nil
		}
		fetched = append(fetched, logFile)
	}

	return fetched, nil
}

func FetchLogs(folder storage.Folder, startTS time.Time, endTS *time.Time, logFolderPath string, handlers LogFetchHandlers) (fetched []storage.Object, err error) {
	logFolder := folder.GetSubFolder(logFolderPath)
	logsToFetch, err := getLogsCoveringInterval(logFolder, startTS, endTS)
	if err != nil {
		return nil, err
	}

	fetched, err = downloadLogFiles(logsToFetch, logFolder, handlers)
	if err != nil {
		return nil, err
	}

	return fetched, nil
}

// TODO : unit tests
func logFileShouldBeFetched(backupStartUploadTime time.Time, endTS *time.Time, object storage.Object) bool {
	return (backupStartUploadTime.Before(object.GetLastModified()) || backupStartUploadTime.Equal(object.GetLastModified())) &&
		(endTS == nil || (*endTS).After(object.GetLastModified()))
}

// TODO : unit tests
func FetchStreamSentinel(backup *Backup, sentinelDto interface{}) error {
	sentinelDtoData, err := backup.fetchSentinelData()
	if err != nil {
		return errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, sentinelDto)
	return errors.Wrap(err, "failed to unmarshal sentinel")
}
