package mongo

import (
	"context"
	"time"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
	"github.com/wal-g/wal-g/utility"
)

type HandleBinaryBackupPushArgs struct {
	AppName       string
	CountJournals bool
	Permanent     bool
	SkipMetadata  bool
}

func HandleBinaryBackupPush(ctx context.Context, args HandleBinaryBackupPushArgs) error {
	mongodbURI, err := conf.GetRequiredSetting(conf.MongoDBUriSetting)
	if err != nil {
		return err
	}
	mongodService, err := binary.CreateMongodService(ctx, args.AppName, mongodbURI, 10*time.Minute)
	if err != nil {
		return err
	}

	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath + "/")

	backupService, err := binary.CreateBackupService(ctx, mongodService, uploader)
	if err != nil {
		return err
	}

	doBackupArgs := binary.DoBackupArgs{
		BackupName:    binary.GenerateNewBackupName(),
		CountJournals: args.CountJournals,
		Permanent:     args.Permanent,
		SkipMetadata:  args.SkipMetadata,
	}
	return backupService.DoBackup(doBackupArgs)
}
