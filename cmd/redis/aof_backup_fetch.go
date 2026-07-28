package redis

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/utility"
)

func runAOFBackupFetch(ctx context.Context, backupName string) error {
	uploader, err := internal.ConfigureUploader(ctx)
	if err != nil {
		return err
	}

	sourceStorageFolder := uploader.Folder()
	uploader.ChangeDirectory(utility.BaseBackupPath + "/")

	return redisdb.HandleAofFetchPush(ctx, sourceStorageFolder, uploader, backupName, redisVersion,
		skipBackupDownloadFlag, skipCheckFlag)
}
