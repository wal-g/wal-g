package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
)

type LogFetchSettings interface {
	GetLogFolderPath() string
	// second value maybe nil, this means upper limit is inf
	GetLogsFetchInterval() (time.Time, *time.Time)
}

type LogFetchHandlers interface {
	FetchLog(logFolder storage.Folder, logName string) (needAbortFetch bool, err error)
	HandleAbortFetch(LogName string) error
	AfterFetch(logs []storage.Object) error
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
		archiveReader, exists, err := TryDownloadFile(backup.BaseBackupFolder, getStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
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
func GetLogsCoveringInterval(folder storage.Folder, start time.Time) ([]storage.Object, error) {
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
		if logFileShouldBeFetched(start, logFile) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	return logsToFetch, nil
}

// TODO : unit tests
// DownloadLogFiles downloads files to specified folder
// FetchLogFiles downloads files to specified folder
func FetchLogFiles(logFiles []storage.Object, logFolder storage.Folder, handlers LogFetchHandlers) ([]storage.Object, error) {
	var fetched []storage.Object
	for _, logFile := range logFiles {
		logName := utility.TrimFileExtension(logFile.GetName())
		if needAbortFetch, err := handlers.FetchLog(logFolder, logName); err != nil {
			return nil, err
		} else if needAbortFetch {
			if err = handlers.HandleAbortFetch(logName); err != nil {
				return nil, err
			}
			return fetched, nil
		}
		fetched = append(fetched, logFile)
	}

	return fetched, nil
}

// TODO : unit tests
func FetchLogs(folder storage.Folder, settings LogFetchSettings, handlers LogFetchHandlers) ([]storage.Object, error) {
	logFolderPath := settings.GetLogFolderPath()
	logFolder := folder.GetSubFolder(logFolderPath)
	startTS, _ := settings.GetLogsFetchInterval()
	logsToFetch, err := GetLogsCoveringInterval(logFolder, startTS)
	if err != nil {
		return nil, err
	}

	fetched, err := FetchLogFiles(logsToFetch, logFolder, handlers)
	if err != nil {
		return nil, err
	}
	if err = handlers.AfterFetch(fetched); err != nil {
		return nil, err
	}
	return fetched, nil
}

// TODO : unit tests
func logFileShouldBeFetched(backupStartUploadTime time.Time, object storage.Object) bool {
	return backupStartUploadTime.Before(object.GetLastModified()) || backupStartUploadTime.Equal(object.GetLastModified())
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

// DownloadFile downloads, decompresses and decrypts
func DownloadFile(folder storage.Folder, filename, ext string, writeCloser io.WriteCloser) error {
	decompressor := compression.FindDecompressor(ext)
	if decompressor == nil {
		return fmt.Errorf("decompressor for extension '%s' was not found", ext)
	}
	archiveReader, exists, err := TryDownloadFile(folder, filename)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("File '%s' does not exist.\n", filename)
	}

	err = DecompressDecryptBytes(&EmptyWriteIgnorer{WriteCloser: writeCloser}, archiveReader, decompressor)
	if err != nil {
		return err
	}
	utility.LoggedClose(writeCloser, "")
	return nil
}
