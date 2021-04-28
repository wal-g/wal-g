package redis

import (
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupPush(uploader *internal.Uploader, backupCmd *exec.Cmd, metaProvider internal.MetaProvider) error {
	stdout, err := utility.StartCommandWithStdoutPipe(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	redisUploader := archive.NewRedisStorageUploader(uploader)

	return redisUploader.UploadBackup(stdout, backupCmd, metaProvider)
}
