package mongo

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"os"
	"path"
	"time"
)

type OpLogFetchSettings struct{}

func (settings OpLogFetchSettings) GetEndTS() (*time.Time, error) {
	return internal.ParseTS(OplogEndTs)
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

func (handlers OpLogFetchHandlers) GetLogFilePath(pathToLog string) (string, error) {
	oplogFileSubFolder := path.Join(handlers.dstPathFolder, pathToLog)
	err := os.MkdirAll(oplogFileSubFolder, os.ModePerm)
	if err != nil {
		return "", err
	}
	oplogFilePath := path.Join(oplogFileSubFolder, "oplog.bson")
	return oplogFilePath, nil
}

func (handlers OpLogFetchHandlers) DownloadLogTo(logFolder storage.Folder, logName string, dstLogFilePath string) error {
	return internal.DownloadWALFileTo(logFolder, logName, dstLogFilePath)
}

func (handlers OpLogFetchHandlers) ShouldBeAborted(pathToLog string) (bool, error) {
	return false, nil
}

func FetchLogs(folder storage.Folder, backup *internal.Backup, settings internal.LogFetchSettings) error {
	var streamSentinel StreamSentinelDto

	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return err
	}

	endTS, err := settings.GetEndTS()
	if err != nil {
		return err
	}

	dstFolder, err := settings.GetDestFolderPath()
	if err != nil {
		return err
	}
	handlers := OpLogFetchHandlers{dstPathFolder: dstFolder}
	_, err = internal.FetchLogs(folder, streamSentinel.StartLocalTime, endTS, settings.GetLogFolderPath(), handlers)
	return err
}

func HandleOplogFetch(folder storage.Folder, backupName string) error {
	if !internal.FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	backup, err := internal.GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %+v\n", err)
	return FetchLogs(folder, backup, OpLogFetchSettings{})
}
