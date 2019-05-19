package mysql

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/config"
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

	tracelog.InfoLogger.Println("stream-fetch")
	streamSentinel, err := backup.FetchStreamSentinel()
	if err != nil {
		return err
	}
	binlogsAreDone := make(chan error)

	go fetchBinlogs(folder, streamSentinel, binlogsAreDone)

	for _, decompressor := range compression.Decompressors {
		d := decompressor
		archiveReader, exists, err := internal.TryDownloadWALFile(baseBackupFolder, getStreamName(&backup, d.FileExtension()))
		if err != nil {
			return err
		}
		if !exists {
			continue
		}

		err = internal.DecompressWALFile(&internal.EmptyWriteIgnorer{WriteCloser: os.Stdout}, archiveReader, d)
		if err != nil {
			return err
		}
		utility.LoggedClose(os.Stdout, "")

		tracelog.DebugLogger.Println("Waiting for binlogs")
		err = <-binlogsAreDone

		return err
	}
	return internal.NewArchiveNonExistenceError(fmt.Sprintf("Archive '%s' does not exist.\n", fileName))
}

func fetchBinlogs(folder storage.Folder, sentinel StreamSentinelDto, binlogsAreDone chan error) {
	binlogFolder := folder.GetSubFolder(BinlogPath)
	endTS, dstFolder := GetBinlogConfigs()
	if dstFolder == "" {
		binlogsAreDone <- errors.New("WALG_MYSQL_BINLOG_DST is not configured")
		return
	}
	objects, _, err := binlogFolder.ListFolder()
	if err != nil {
		binlogsAreDone <- nil
		return
	}
	var fetchedLogs []storage.Object

	for _, object := range objects {
		tracelog.InfoLogger.Println("Consider binlog ", object.GetName(), object.GetLastModified().Format(time.RFC3339))

		binlogName := ExtractBinlogName(object, folder)

		if BinlogShouldBeFetched(sentinel, binlogName, endTS, object) {
			fileName := path.Join(dstFolder, binlogName)
			tracelog.InfoLogger.Println("Download", binlogName, "to", fileName)
			err := internal.DownloadWALFileTo(binlogFolder, binlogName, fileName)
			if err != nil {
				binlogsAreDone <- err
				return
			}
			fetchedLogs = append(fetchedLogs, object)
		}
	}

	sort.Slice(fetchedLogs, func(i, j int) bool {
		return fetchedLogs[i].GetLastModified().After(fetchedLogs[j].GetLastModified())
	})

	index_file, err := os.Create(filepath.Join(dstFolder, "binlogs_order"))
	if err != nil {
		binlogsAreDone <- err
		return
	}

	for _, object := range fetchedLogs {
		_, err := index_file.WriteString(ExtractBinlogName(object, folder) + "\n")
		if err != nil {
			binlogsAreDone <- err
			return
		}
	}
	err = index_file.Close()
	if err != nil {
		binlogsAreDone <- err
		return
	}

	binlogsAreDone <- nil
}

func BinlogShouldBeFetched(sentinel StreamSentinelDto, binlogName string, endTS *time.Time, object storage.Object) bool {
	return sentinel.BinLogStart <= binlogName && (endTS == nil || (*endTS).After(object.GetLastModified()))
}

func GetBinlogConfigs() (*time.Time, string) {
	endTSStr := config.GetSettingValue(BinlogEndTs)
	var endTS *time.Time
	if endTSStr != "" {
		if t, err := time.Parse(time.RFC3339, endTSStr); err == nil {
			endTS = &t
		}
	}
	dstFolder := config.GetSettingValue(BinlogDst)
	return endTS, dstFolder
}

func ExtractBinlogName(object storage.Object, folder storage.Folder) string {
	binlogName := object.GetName()
	return strings.TrimSuffix(binlogName, "."+utility.GetFileExtension(binlogName))
}
