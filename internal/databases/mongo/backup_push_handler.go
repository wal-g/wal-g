package mongo

import (
	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/storage"
	"github.com/wal-g/wal-g/utility"
)

func HandleStreamPush(uploader *storage.Uploader, command []string) {
	waitAndFatalOnError, stream := internal.StartCommand(command)
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	err := uploader.UploadStream(stream)
	tracelog.ErrorLogger.FatalOnError(err)
	waitAndFatalOnError()
}
