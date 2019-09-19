package redis

import (
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"io"
	"io/ioutil"
	"time"
)

func HandleStreamPush(uploader *Uploader, command []string) {
	// Configure folder
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	waitFunc, stream, errorStream := internal.StartCommand(command)
	backupName := "dump_" + time.Now().Format(time.RFC3339)
	err := uploader.UploadStream(backupName, stream)
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

func (uploader *Uploader) UploadStream(backupName string, stream io.Reader) error {
	compressed := internal.CompressAndEncrypt(stream, uploader.Compressor, internal.ConfigureCrypter())

	err := uploader.Upload(backupName, compressed)

	return err
}
