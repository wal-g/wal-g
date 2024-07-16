package redis

import (
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/rdb"
	"github.com/wal-g/wal-g/utility"
)

func HandleRDBBackupPush(uploader internal.Uploader, backupCmd *exec.Cmd, metaConstructor internal.MetaConstructor) error {
	stdout, err := utility.StartCommandWithStdoutPipe(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	redisUploader := rdb.NewRedisStorageUploader(uploader)

	return redisUploader.UploadBackup(stdout, backupCmd, metaConstructor)
}
