package binary

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
)

type BackupService struct {
	Context       context.Context
	MongodService *MongodService
	Uploader      *internal.Uploader

	Sentinel models.Backup
}

func GenerateNewBackupName() string {
	return common.BinaryBackupType + "_" + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

func CreateBackupService(ctx context.Context, mongodService *MongodService, uploader *internal.Uploader,
) (*BackupService, error) {
	return &BackupService{
		Context:       ctx,
		MongodService: mongodService,
		Uploader:      uploader,
	}, nil
}

func (backupService *BackupService) DoBackup(backupName string, permanent bool) error {
	err := backupService.InitializeMongodBackupMeta(backupName, permanent)
	if err != nil {
		return err
	}

	backupCursor, err := CreateBackupCursor(backupService.MongodService)
	if err != nil {
		return errors.Wrap(err, "unable to open backup cursor")
	}
	defer backupCursor.Close()

	backupFiles, err := backupCursor.LoadBackupCursorFiles()
	if err != nil {
		return errors.Wrapf(err, "unable to load data from backup cursor")
	}

	backupCursor.StartKeepAlive()

	mongodDBPath := backupCursor.BackupCursorMeta.DBPath
	concurrentUploader, err := CreateConcurrentUploader(backupService.Uploader, backupName, mongodDBPath)
	if err != nil {
		return err
	}

	err = concurrentUploader.UploadBackupFiles(backupFiles)
	if err != nil {
		return errors.Wrapf(err, "unable to upload backup files")
	}

	extendedBackupFiles, err := backupCursor.LoadExtendedBackupCursorFiles()
	if err != nil {
		return errors.Wrapf(err, "unable to load data from backup cursor")
	}

	err = concurrentUploader.UploadBackupFiles(extendedBackupFiles)
	if err != nil {
		return errors.Wrapf(err, "unable to upload backup files")
	}

	err = concurrentUploader.Finalize()
	if err != nil {
		return err
	}

	return backupService.Finalize(concurrentUploader, backupCursor.BackupCursorMeta)
}

func (backupService *BackupService) InitializeMongodBackupMeta(backupName string, permanent bool) error {
	mongodVersion, err := backupService.MongodService.MongodVersion()
	if err != nil {
		return err
	}

	userData, err := internal.GetSentinelUserData()
	if err != nil {
		return err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	backupService.Sentinel.BackupName = backupName
	backupService.Sentinel.BackupType = common.BinaryBackupType
	backupService.Sentinel.Hostname = hostname
	backupService.Sentinel.MongoMeta.Version = mongodVersion
	backupService.Sentinel.StartLocalTime = utility.TimeNowCrossPlatformLocal()
	backupService.Sentinel.Permanent = permanent
	backupService.Sentinel.UserData = userData

	return nil
}

func (backupService *BackupService) Finalize(uploader *ConcurrentUploader, backupCursorMeta *BackupCursorMeta) error {
	sentinel := &backupService.Sentinel
	sentinel.FinishLocalTime = utility.TimeNowCrossPlatformLocal()
	sentinel.UncompressedSize = uploader.UncompressedSize
	sentinel.CompressedSize = uploader.CompressedSize

	sentinel.MongoMeta.BackupLastTS = backupCursorMeta.OplogEnd.TS
	backupLastTS := models.TimestampFromBson(sentinel.MongoMeta.BackupLastTS)
	sentinel.MongoMeta.Before.LastMajTS = backupLastTS
	sentinel.MongoMeta.Before.LastTS = backupLastTS
	sentinel.MongoMeta.After.LastMajTS = backupLastTS
	sentinel.MongoMeta.After.LastTS = backupLastTS

	return internal.UploadSentinel(backupService.Uploader, sentinel, sentinel.BackupName)
}
