package mongo

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"os"
	"path"
	"time"
)

type OpLogFetchSettings struct {
	startTs time.Time
	endTS   *time.Time
}

func (settings OpLogFetchSettings) GetLogsFetchInterval() (time.Time, *time.Time) {
	return settings.startTs, settings.endTS
}

func (settings OpLogFetchSettings) GetDestFolderPath() (string, error) {
	return internal.GetLogsDstSettings(OplogDst)
}

func (settings OpLogFetchSettings) GetLogFolderPath() string {
	return OplogPath
}

type OpLogFetchHandlers struct {
	dstPathFolder string
}

func (handlers OpLogFetchHandlers) HandleAbortFetch(logFilePath string) error {
	return os.Remove(logFilePath)
}

func GetLogFilePath(dstPathFolder, pathToLog string) (string, error) {
	oplogFileSubFolder := path.Join(dstPathFolder, pathToLog)
	err := os.MkdirAll(oplogFileSubFolder, os.ModePerm)
	if err != nil {
		return "", err
	}
	oplogFilePath := path.Join(oplogFileSubFolder, "oplog.bson")
	return oplogFilePath, nil
}

// TODO : unit tests
func (handlers OpLogFetchHandlers) FetchLog(logFolder storage.Folder, logName string) (needAbortFetch bool, err error) {
	pathToLog, err := GetLogFilePath(handlers.dstPathFolder, logName)
	if err != nil {
		return true, nil
	}
	return false, internal.DownloadWALFileTo(logFolder, logName, pathToLog)
}

func (handlers OpLogFetchHandlers) AfterFetch([]storage.Object) error {
	return nil
}

// TODO : unit tests
func FetchLogs(folder storage.Folder, backup *internal.Backup) error {
	var streamSentinel StreamSentinelDto
	if err := internal.FetchStreamSentinel(backup, &streamSentinel); err != nil {
		return err
	}
	endTS, err := internal.ParseTS(OplogEndTs)
	if err != nil {
		return err
	}
	settings := OpLogFetchSettings{
		startTs: streamSentinel.StartLocalTime,
		endTS:   endTS,
	}

	dstPathFolder, err := settings.GetDestFolderPath()
	if err != nil {
		return err
	}
	handlers := OpLogFetchHandlers{dstPathFolder: dstPathFolder}
	_, err = internal.FetchLogs(folder, settings, handlers)
	return err
}

// TODO : unit tests
func HandleOplogFetch(folder storage.Folder, backupName string) error {
	if !internal.FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	backup, err := internal.GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %+v\n", err)
	return FetchLogs(folder, backup)
}
