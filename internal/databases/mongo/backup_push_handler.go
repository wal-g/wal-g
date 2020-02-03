package mongo

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"

	"github.com/wal-g/tracelog"
)

// HandleStreamPush starts backup procedure.
func HandleStreamPush(uploader archive.Uploader, command []string, metaProvider archive.BackupMetaProvider) {
	err := metaProvider.Init()
	tracelog.ErrorLogger.FatalOnError(err)
	waitAndFatalOnError, stream := internal.StartCommand(command)
	err = uploader.UploadBackup(stream, metaProvider)
	tracelog.ErrorLogger.FatalOnError(err)
	waitAndFatalOnError()
}
