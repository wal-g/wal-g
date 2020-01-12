package redis

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"io"
	"time"
)

func HandleStreamPush(uploader *Uploader, command []string) {
	// Configure folder
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	waitAndFatalOnError, stream := internal.StartCommand(command)
	backupName := "dump_" + time.Now().Format(time.RFC3339)
	err := uploader.UploadStream(backupName, stream)
	tracelog.ErrorLogger.FatalOnError(err)
	waitAndFatalOnError()
}

func (uploader *Uploader) UploadStream(backupName string, stream io.Reader) error {
	compressed := internal.CompressAndEncrypt(stream, uploader.Compressor, internal.ConfigureCrypter())

	err := uploader.Upload(backupName, compressed)

	return err
}
