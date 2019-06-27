package mongo

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamFetch(backupName string, folder storage.Folder) {
	backup, err := internal.GetBackupByName(backupName, folder)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Unable to get backup %+v\n", err)
	}
	if !internal.FileIsPiped(os.Stdout) {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	err = downloadAndDecompressStream(folder, backup)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
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

// TODO : unit tests
func downloadAndDecompressStream(folder storage.Folder, backup *internal.Backup) error {
	streamSentinel, err := FetchStreamSentinel(backup)
	if err != nil {
		return err
	}
	oplogsAreDone := make(chan error)
	go fetchOplogs(folder, streamSentinel.StartLocalTime, oplogsAreDone)

	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := internal.TryDownloadWALFile(backup.BaseBackupFolder, getStreamName(backup.Name, decompressor.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = internal.DecompressWALFile(&internal.EmptyWriteIgnorer{WriteCloser: os.Stdout}, archiveReader, decompressor)
		if err != nil {
			return err
		}
		utility.LoggedClose(os.Stdout, "")
		tracelog.DebugLogger.Println("Waiting for oplogs")
		err = <-oplogsAreDone
		return err
	}
	return internal.NewArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", backup.Name))
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
