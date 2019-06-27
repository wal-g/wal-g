package mongo

import (
	"encoding/json"
	"path"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func FetchBackupStreamAndOplog(folder storage.Folder, backup *internal.Backup) error {
	streamSentinel, err := FetchStreamSentinel(backup)
	if err != nil {
		return err
	}
	oplogsAreDone := make(chan error)
	go fetchOplogs(folder, streamSentinel.StartLocalTime, oplogsAreDone)
	err = internal.DownloadAndDecompressStream(folder, backup)
	if err != nil {
		return err
	}
	tracelog.DebugLogger.Println("Waiting for oplogs")
	err = <-oplogsAreDone
	return err
}

// TODO : unit tests
func FetchStreamSentinel(backup *internal.Backup) (StreamSentinelDto, error) {
	sentinelDto := StreamSentinelDto{}
	sentinelDtoData, err := backup.FetchSentinelData()
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
}

func fetchOplogs(folder storage.Folder, startTime time.Time, oplogAreDone chan error) {
	oplogFolder := folder.GetSubFolder(OplogPath)
	endTS, oplogDstFolder, err := getOplogConfigs()
	if err != nil {
		oplogAreDone <- nil
		return
	}
	oplogFiles, _, err := oplogFolder.ListFolder()
	if err != nil {
		oplogAreDone <- err
		return
	}

	sort.Slice(oplogFiles, func(i, j int) bool {
		return oplogFiles[i].GetLastModified().After(oplogFiles[j].GetLastModified())
	})

	for _, oplogFile := range oplogFiles {
		if oplogFile.GetLastModified().After(startTime) {
			oplogName := utility.TrimFileExtension(oplogFile.GetName())
			oplogFilePath, err := getOplogDstFilePath(oplogName, oplogDstFolder)
			if err != nil {
				oplogAreDone <- err
				return
			}

			err = internal.DownloadWALFileTo(oplogFolder, oplogName, oplogFilePath)
			if err != nil {
				oplogAreDone <- err
				return
			}
			tracelog.InfoLogger.Println("oplog file " + oplogFile.GetName() + " fetched to " + oplogFilePath)

			if endTS != nil && oplogFile.GetLastModified().After(*endTS) {
				break
			}
		}
	}

	oplogAreDone <- nil
}

func getOplogDstFilePath(oplogName string, oplogDstFolder string) (string, error) {
	oplogFileSubFolder := path.Join(oplogDstFolder, oplogName)
	_, err := internal.NewDiskDataFolder(oplogFileSubFolder)
	if err != nil {
		return "", err
	}
	return path.Join(oplogFileSubFolder, "oplog.bson"), nil
}

func getOplogConfigs() (*time.Time, string, error) {
	return internal.GetOperationLogsSettings(OplogEndTs, OplogDst)
}
