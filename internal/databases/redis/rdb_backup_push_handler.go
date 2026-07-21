package redis

import (
	"context"
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/rdb"
	"github.com/wal-g/wal-g/utility"
)

type RDBBackupPushArgs struct {
	BackupCmd       *exec.Cmd
	BackupName      string
	Sharded         bool
	Uploader        internal.Uploader
	MetaConstructor internal.MetaConstructor
	DeferSentinel   bool
}

func HandleRDBBackupPush(ctx context.Context, args RDBBackupPushArgs) error {
	stdout, err := utility.StartCommandWithStdoutPipe(args.BackupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	redisUploader := rdb.NewRedisStorageUploader(args.Uploader)
	backupName := args.BackupName
	if backupName == "" {
		backupName = rdb.GenerateNewBackupName()
	}
	uploadArgs := rdb.UploadBackupArgs{
		BackupName:      backupName,
		Cmd:             args.BackupCmd,
		MetaConstructor: args.MetaConstructor,
		Sharded:         args.Sharded,
		Stream:          stdout,
		DeferSentinel:   args.DeferSentinel,
	}

	return redisUploader.UploadBackup(ctx, uploadArgs)
}
