package mysql

import (
	"database/sql"
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
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")
	var stream io.Reader = os.Stdin
	if internal.FileIsPiped(os.Stdin) {
		tracelog.InfoLogger.Println("Data is piped from stdin")
	} else {
		tracelog.ErrorLogger.Println("WARNING: stdin is terminal: operating in test mode!")
		stream = strings.NewReader("testtesttest")
	}
	err = uploader.UploadStream(db, stream)
	tracelog.ErrorLogger.FatalOnError(err)
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadStream(db *sql.DB, stream io.Reader) error {
	binlogStart := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog start file", binlogStart)
	timeStart := utility.TimeNowCrossPlatformLocal()

	fileName, err := uploader.PushStream(stream)
	if err != nil {
		return err
	}

	binlogEnd := getMySQLCurrentBinlogFile(db)
	tracelog.DebugLogger.Println("Binlog end file", binlogEnd)

	sentinel := StreamSentinelDto{BinLogStart: binlogStart, BinLogEnd: binlogEnd, StartLocalTime: timeStart}
	return internal.UploadSentinel(uploader.Uploader, &sentinel, fileName)
}
