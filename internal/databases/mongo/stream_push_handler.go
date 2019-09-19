package mongo

import (
	"github.com/wal-g/wal-g/internal"
	"io"
	"io/ioutil"

	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamPush(uploader *Uploader, command []string) {
	waitFunc, stream, errorStream := internal.StartCommand(command)
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	err := uploader.UploadStream(stream)
	tracelog.ErrorLogger.FatalOnError(err)
	var errorString string
	if errorBytes, err := ioutil.ReadAll(errorStream); err == nil {
		errorString = string(errorBytes)
	}
	tracelog.ErrorLogger.FatalOnError(err)

	err = waitFunc()
	if err != nil {
		tracelog.ErrorLogger.Println(errorString)
		tracelog.ErrorLogger.FatalOnError(err)
	}

}

// TODO : unit tests
// UploadStream compresses a stream and uploads it.
func (uploader *Uploader) UploadStream(stream io.Reader) error {
	timeStart := utility.TimeNowCrossPlatformLocal()
	backupName, err := uploader.PushStream(stream)
	if err != nil {
		return err
	}
	return internal.UploadSentinel(uploader.Uploader, &StreamSentinelDto{StartLocalTime: timeStart}, backupName)
}
