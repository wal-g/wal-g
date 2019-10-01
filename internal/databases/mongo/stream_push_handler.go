package mongo

import (
	"github.com/wal-g/wal-g/internal"
	"io"

	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamPush(uploader *Uploader, command []string) {
	waitAndFatalOnError, stream := internal.StartCommand(command)
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	err := uploader.UploadStream(stream)
	tracelog.ErrorLogger.FatalOnError(err)
	waitAndFatalOnError()
}

// TODO : unit tests
// UploadStream compresses a stream and uploads it.
func (uploader *Uploader) UploadStream(stream io.Reader) error {
	timeStart := utility.TimeNowCrossPlatformLocal()
	backupName, err := uploader.PushStream(stream)
	if err != nil {
		return err
	}
	currentBackupSentinelDto := &StreamSentinelDto{
		StartLocalTime:  timeStart,
		FinishLocalTime: utility.TimeNowCrossPlatformLocal(),
		UserData:        internal.GetSentinelUserData(),
	}
	return internal.UploadSentinel(uploader.Uploader, currentBackupSentinelDto, backupName)
}
