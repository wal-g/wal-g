package mongo

import (
	"os"
	"path"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
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

type OpLogFetchParams struct {
	folder  storage.Folder
	startTS time.Time
}

func (params OpLogFetchParams) GetStorageFolder() storage.Folder {
	return params.folder
}

func (params OpLogFetchParams) GetStartTs() time.Time {
	return params.startTS
}

type OpLogFetchHandlers struct {
	dstFolder string
}

func (handlers OpLogFetchHandlers) GetLogFilePath(pathToLog string) (string, error) {
	oplogFileSubFolder := path.Join(handlers.dstFolder, pathToLog)
	err := os.MkdirAll(oplogFileSubFolder, os.ModePerm)
	if err != nil {
		return "", err
	}
	oplogFilePath := path.Join(oplogFileSubFolder, "oplog.bson")
	return oplogFilePath, nil
}

func (handlers OpLogFetchHandlers) CheckUploadedLog(pathToLog string) (bool, error) {
	return false, nil
}

func FetchLogs(folder storage.Folder, backup *internal.Backup) error {
	var streamSentinel StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return err
	}

	_, dstFolder, err := internal.GetOperationLogsSettings(OpLogFetchSettings{})

	params := OpLogFetchParams{folder: folder, startTS: streamSentinel.StartLocalTime}
	handlers := OpLogFetchHandlers{dstFolder: dstFolder}

	_, err = internal.FetchLogs(params, OpLogFetchSettings{}, handlers)
	return err
}
