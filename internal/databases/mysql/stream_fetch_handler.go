package mysql

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"os"
	"path"
	"strings"
	"time"
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
	baseBackupFolder := folder.GetSubFolder(internal.BaseBackupPath)
	backup := Backup{internal.NewBackup(baseBackupFolder, fileName)}

	tracelog.InfoLogger.Println("stream-fetch")
	streamSentinel, err := backup.FetchStreamSentinel()
	if err != nil {
		return err
	}
	binlogsAreDone := make(chan error)

	go fetchBinlogs(folder, streamSentinel, binlogsAreDone)

	for _, decompressor := range internal.Decompressors {
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
		internal.LoggedClose(os.Stdout)

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
		}
	}

	binlogsAreDone <- nil
}

func BinlogShouldBeFetched(sentinel StreamSentinelDto, binlogName string, endTS *time.Time, object storage.Object) bool {
	return sentinel.BinLogStart <= binlogName && (endTS == nil || (*endTS).After(object.GetLastModified()))
}

func GetBinlogConfigs() (*time.Time, string) {
	endTSStr := internal.GetSettingValue(BinlogEndTs)
	var endTS *time.Time
	if endTSStr != "" {
		if t, err := time.Parse(time.RFC3339, endTSStr); err == nil {
			endTS = &t
		}
	}
	dstFolder := internal.GetSettingValue(BinlogDst)
	return endTS, dstFolder
}

func ExtractBinlogName(object storage.Object, folder storage.Folder) string {
	binlogName := object.GetName()
	return strings.TrimSuffix(binlogName, "."+internal.GetFileExtension(binlogName))
}