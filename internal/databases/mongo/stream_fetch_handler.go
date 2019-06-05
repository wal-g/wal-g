package mongo

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamFetch(backupName string, folder storage.Folder) {
	if backupName == "" || backupName == "LATEST" {
		latest, err := internal.GetLatestBackupName(folder)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Unable to get latest backup %+v\n", err)
		}
		backupName = latest
	}
	stat, _ := os.Stdout.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
	} else {
		tracelog.ErrorLogger.Fatalf("stdout is a terminal")
	}
	err := downloadAndDecompressStream(folder, backupName)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
func downloadAndDecompressStream(folder storage.Folder, fileName string) error {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	backup := Backup{internal.NewBackup(baseBackupFolder, fileName)}

	streamSentinel, err := backup.FetchStreamSentinel()
	if err != nil {
		return err
	}
	err = fetchOplogs(folder, streamSentinel.StartLocalTime)
	if err != nil {
		return err
	}

	for _, decompressor := range compression.Decompressors {
		archiveReader, exists, err := internal.TryDownloadWALFile(baseBackupFolder, getStreamName(backup.Name, decompressor.FileExtension()))
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

		return err
	}
	return internal.NewArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", fileName))
}

func fetchOplogs(folder storage.Folder, startTime time.Time) error {
	oplogFolder := folder.GetSubFolder(OplogPath)
	endTS, oplogDstFolder := getOplogConfigs()
	if oplogDstFolder == "" {
		return errors.New("WALG_MONGO_OPLOG_DST is not configured")
	}
	oplogFiles, _, err := oplogFolder.ListFolder()
	if err != nil {
		return err
	}

	sort.Slice(oplogFiles, func(i, j int) bool {
		return oplogFiles[i].GetLastModified().After(oplogFiles[j].GetLastModified())
	})

	for _, oplogFile := range oplogFiles {
		if oplogFile.GetLastModified().After(startTime) {

			oplogName := extractOplogName(oplogFile.GetName())

			fileName := path.Join(oplogDstFolder, oplogName) + ".bson"

			err = internal.DownloadWALFileTo(oplogFolder, oplogName, fileName)

			tracelog.InfoLogger.Println("oplog file " + oplogFile.GetName() + " fetched to " + fileName)

			if endTS != nil && oplogFile.GetLastModified().After(*endTS) {
				break
			}
		}
	}

	return err
}

func extractOplogName(filename string) string {
	return strings.TrimSuffix(filename, "."+utility.GetFileExtension(filename))
}

func getOplogConfigs() (*time.Time, string) {
	endTSStr, ok := internal.GetSetting(OplogEndTs)
	var endTS *time.Time
	if ok {
		if t, err := time.Parse(time.RFC3339, endTSStr); err == nil {
			endTS = &t
		}
	}
	dstFolder, ok := internal.GetSetting(OplogDst)
	return endTS, dstFolder
}
