package redis

import (
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/rdb"
	"github.com/wal-g/wal-g/utility"
)

type RDBBackupPushArgs struct {
	BackupCmd       *exec.Cmd
	Sharded         bool
	Uploader        internal.Uploader
	MetaConstructor internal.MetaConstructor
}

func HandleRDBBackupPush(args RDBBackupPushArgs) error {
	stdout, err := utility.StartCommandWithStdoutPipe(args.BackupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	valkeyUploader := rdb.NewValkeyStorageUploader(args.Uploader)
	uploadArgs := rdb.UploadBackupArgs{
		Cmd:             args.BackupCmd,
		MetaConstructor: args.MetaConstructor,
		Sharded:         args.Sharded,
		Stream:          stdout,
	}

	return valkeyUploader.UploadBackup(uploadArgs)
}
