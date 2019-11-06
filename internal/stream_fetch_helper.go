package internal

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"sort"
	"time"

	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
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

// GetLogsDstSettings reads from the environment variables fetch settings
func GetLogsDstSettings(operationLogsDstEnvVariable string) (dstFolder string, err error) {
	dstFolder, ok := GetSetting(operationLogsDstEnvVariable)
	if !ok {
		return dstFolder, NewUnsetRequiredSettingError(operationLogsDstEnvVariable)
	}
	return dstFolder, nil
}

// DownloadAndDecompressStream downloads, decompresses and writes stream to stdout
func DownloadAndDecompressStream(backup *Backup, writeCloser io.WriteCloser) error {
	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := TryDownloadWALFile(backup.BaseBackupFolder, getStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = DecompressWALFile(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
		if err != nil {
			return err
		}
		utility.LoggedClose(writeCloser, "")
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
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].GetLastModified().Before(logFiles[j].GetLastModified())
	})

	var logsToFetch []storage.Object
	for _, logFile := range logFiles {
		tracelog.InfoLogger.Println("Consider log file: ", logFile.GetName(), logFile.GetLastModified().Format(time.RFC3339))
		if LogFileShouldBeFetched(start, end, logFile) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	return logsToFetch, nil
}

// DownloadLogFiles downloads files to specified folder
func DownloadLogFiles(logFiles []storage.Object, logFolder storage.Folder, handlers LogFetchHandlers) ([]storage.Object, error) {
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
	logsToFetch, err := GetLogsCoveringInterval(logFolder, startTS, endTS)
	if err != nil {
		return nil, err
	}

	fetched, err = DownloadLogFiles(logsToFetch, logFolder, handlers)
	if err != nil {
		return nil, err
	}

	return fetched, nil
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
