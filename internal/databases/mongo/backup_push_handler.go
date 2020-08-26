package mongo

import (
	"os/exec"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
)

// HandleBackupPush starts backup procedure.
func HandleBackupPush(uploader archive.Uploader, metaProvider archive.MongoMetaProvider, permanent bool, backupCmd *exec.Cmd) error {
	err := metaProvider.Init(permanent)
	tracelog.ErrorLogger.FatalOnError(err)
	stdout, err := utility.StartCommandWithStdoutPipe(backupCmd)
	tracelog.ErrorLogger.FatalOnError(err)
	return uploader.UploadBackup(stdout, backupCmd, metaProvider)
}
