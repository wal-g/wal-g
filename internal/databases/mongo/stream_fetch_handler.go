package mongo

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"os"
	"path"
)

type OpLogFetchSettings struct{}

func (settings OpLogFetchSettings) GetEndTsEnv() string {
	return OplogEndTs
}

func (settings OpLogFetchSettings) GetDstEnv() string {
	return OplogDst
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

func FetchLogs(folder storage.Folder, backup *internal.Backup) error {
	var streamSentinel StreamSentinelDto
	var opLogFetchSettings OpLogFetchSettings

	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return err
	}

	endTS, dstFolder, err := internal.GetOperationLogsSettings(opLogFetchSettings)

	handlers := OpLogFetchHandlers{dstPathFolder: dstFolder}

	_, err = internal.FetchLogs(folder, streamSentinel.StartLocalTime, endTS, opLogFetchSettings, handlers)
	return err
}
