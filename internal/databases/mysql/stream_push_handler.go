package mysql

import (
	"bytes"
	"database/sql"
	"io"
	"os"
	"os/exec"

	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamPush(uploader *Uploader, command []string) {
	if len(command) == 0{
		tracelog.ErrorLogger.Println("WARNING: command not specified")
		os.Exit(1)
	}
	waitFunc, stream, stderr := startCommand(command)
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")
	err = uploader.UploadStream(db, stream)
	tracelog.ErrorLogger.FatalOnError(err)
	err = waitFunc()
	tracelog.ErrorLogger.FatalOnError(err)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(stderr)
	s := buf.String()
	if s != ""{
		tracelog.ErrorLogger.Println("ERROR: Stderr of the command is not empty.")
		os.Exit(1)
	}
}

func startCommand(command []string) (waitFunc func() error, stdout, stderr io.ReadCloser) {
	c := exec.Command(command[0], command[1:]...)
	stdout, err := c.StdoutPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	stderr, err = c.StderrPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	err = c.Start()
	waitFunc = c.Wait
	tracelog.ErrorLogger.FatalOnError(err)
	return
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
