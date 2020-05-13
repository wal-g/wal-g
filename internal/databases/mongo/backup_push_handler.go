package mongo

import (
	"os/exec"

	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
)

// HandleBackupPush starts backup procedure.
func HandleBackupPush(uploader archive.Uploader, metaProvider archive.MongoMetaProvider, backupCmd *exec.Cmd) {
	err := metaProvider.Init()
	tracelog.ErrorLogger.FatalOnError(err)
	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalOnError(err)
	err = uploader.UploadBackup(stdout, backupCmd, metaProvider)
	if err != nil {
		tracelog.ErrorLogger.Print("Backup command output:\n" + stderr.String())
		tracelog.ErrorLogger.Fatal(err)
	}
}
