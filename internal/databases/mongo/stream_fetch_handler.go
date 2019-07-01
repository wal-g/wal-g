package mongo

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
)

func FetchBackupStreamAndOplog(folder storage.Folder, backup *internal.Backup) error {
	streamSentinel, err := fetchStreamSentinel(backup)
	if err != nil {
		return err
	}
	oplogsAreDone := make(chan error)
	go fetchOplogs(folder, streamSentinel.StartLocalTime, oplogsAreDone)
	err = internal.DownloadAndDecompressStream(backup)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Waiting for oplogs")
	err = <-oplogsAreDone
	return err
}

func fetchStreamSentinel(backup *internal.Backup) (StreamSentinelDto, error) {
	sentinelDto := StreamSentinelDto{}
	sentinelDtoData, err := backup.FetchSentinelData()
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
}

func fetchOplogs(folder storage.Folder, startTime time.Time, oplogAreDone chan error) {
	endTS, oplogDstFolder, err := getOplogConfigs()
	if err != nil {
		oplogAreDone <- nil
		return
	}
	oplogFolder := folder.GetSubFolder(OplogPath)
	logsToFetch, err := internal.GetOperationLogsCoveringInterval(oplogFolder, startTime, endTS)
	if err != nil {
		oplogAreDone <- nil
		return
	}

	oplogAreDone <- internal.DownloadOplogFiles(logsToFetch, oplogFolder, oplogDstFolder, "oplog.bson")
}

func getOplogConfigs() (*time.Time, string, error) {
	return internal.GetOperationLogsSettings(OplogEndTs, OplogDst)
}
