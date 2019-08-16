package mongo

import (
	"os"
	"path"

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

func (settings OpLogFetchSettings) GetFilePath(dstFolder string, logName string) (string, error) {
	oplogFileSubFolder := path.Join(dstFolder, logName)
	err := os.MkdirAll(oplogFileSubFolder, os.ModePerm)
	if err != nil {
		return "", err
	}
	oplogFilePath := path.Join(oplogFileSubFolder, "oplog.bson")
	return oplogFilePath, nil
}

func FetchLogs(folder storage.Folder, backup *internal.Backup) error {
	var streamSentinel StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return err
	}
	_, err = internal.FetchLogs(folder, streamSentinel.StartLocalTime, OpLogFetchSettings{}, func(s string) bool {
		return false
	})
	return err
}
