package mysql

import (
	"database/sql"
	"io"
	"io/ioutil"

	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamPush(uploader *Uploader, command []string) {
	waitFunc, stream, errorStream:= internal.StartCommand(command)
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")
	err = uploader.UploadStream(db, stream)
	tracelog.ErrorLogger.FatalOnError(err)
	var errorString string
	if errorBytes, err := ioutil.ReadAll(errorStream); err == nil {
		errorString = string(errorBytes)
	}
	err = waitFunc()
	if err == nil {
		tracelog.ErrorLogger.Println(errorString)
		tracelog.ErrorLogger.FatalOnError(err)
	}

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
