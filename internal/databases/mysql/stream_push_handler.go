package mysql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamPush(uploader *Uploader) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	db, err := getMySQLConnection()
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	defer utility.LoggedClose(db, "")
	var stream io.Reader = os.Stdin
	if internal.FileIsPiped(os.Stdin) {
		tracelog.InfoLogger.Println("Data is piped from stdin")
	} else {
		tracelog.ErrorLogger.Println("WARNING: stdin is terminal: operating in test mode!")
		stream = strings.NewReader("testtesttest")
	}
	err = uploader.UploadStream(db, stream)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadStream(db *sql.DB, stream io.Reader) error {
	binlogStart := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog start file", binlogStart)
	timeStart := utility.TimeNowCrossPlatformLocal()

	fileName, err := uploader.PushStream(stream)

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
