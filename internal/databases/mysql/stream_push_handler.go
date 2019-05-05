package mysql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
	"strings"
	"time"
)

func HandleStreamPush(uploader *Uploader) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	db, err := getMySQLConnection()
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}

	backupName := StreamPrefix + time.Now().UTC().Format("20060102T150405Z")
	stat, _ := os.Stdin.Stat()
	var stream io.Reader = os.Stdin
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		tracelog.InfoLogger.Println("Data is piped from stdin")
	} else {
		tracelog.ErrorLogger.Println("WARNING: stdin is terminal: operating in test mode!")
		stream = strings.NewReader("testtesttest")
	}
	err = uploader.UploadStream(backupName, db, stream)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadStream(fileName string, db *sql.DB, stream io.Reader) error {
	binlogStart := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog start file", binlogStart)
	timeStart := time.Now()
	compressor := uploader.Compressor

	compressed := internal.CompressAndEncrypt(stream, compressor, internal.NewOpenPGPCrypter())
	backup := Backup{internal.NewBackup(uploader.UploadingFolder, fileName)}

	dstPath := getStreamName(&backup, compressor.FileExtension())
	tracelog.DebugLogger.Println("Upload path", dstPath)

	err := uploader.Upload(dstPath, compressed)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	binlogEnd := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog end file", binlogEnd)

	uploadStreamSentinel(&StreamSentinelDto{BinLogStart: binlogStart, BinLogEnd: binlogEnd, StartLocalTime: timeStart}, uploader, fileName+utility.SentinelSuffix)

	return err
}

func uploadStreamSentinel(sentinelDto *StreamSentinelDto, uploader *Uploader, name string) error {
	dtoBody, err := json.Marshal(*sentinelDto)
	if err != nil {
		return err
	}

	uploadingErr := uploader.Upload(name, bytes.NewReader(dtoBody))
	if uploadingErr != nil {
		tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", name)
		tracelog.ErrorLogger.Fatalf("StorageTarBall finish: json failed to upload")
		return uploadingErr
	}
	return nil
}
