package binary

import (
	"context"
	"github.com/wal-g/tracelog"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
)

type BackupService struct {
	Context       context.Context
	MongodService *MongodService
	Uploader      internal.Uploader

	Sentinel         models.Backup
	BackupRoutesInfo models.BackupRoutesInfo
}

func GenerateNewBackupName() string {
	return common.BinaryBackupType + "_" + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

func CreateBackupService(ctx context.Context, mongodService *MongodService, uploader internal.Uploader,
) (*BackupService, error) {
	return &BackupService{
		Context:       ctx,
		MongodService: mongodService,
		Uploader:      uploader,
	}, nil
}

func (backupService *BackupService) DoBackup(backupName string, permanent, skipMetadata bool) error {
	err := backupService.InitializeMongodBackupMeta(backupName, permanent)
	if err != nil {
		return err
	}

	var backupRoutes *models.BackupRoutesInfo
	if !skipMetadata {
		backupRoutes, err = CreateBackupRoutesInfo(backupService.MongodService)
		if err != nil {
			return err
		}
		backupService.BackupRoutesInfo = *backupRoutes
	}

	backupCursor, err := CreateBackupCursor(backupService.MongodService)
	if err != nil {
		return err
	}
	defer backupCursor.Close()

	backupFiles, err := backupCursor.LoadBackupCursorFiles()
	if err != nil {
		return errors.Wrapf(err, "unable to load data from backup cursor")
	}

	backupCursor.StartKeepAlive()

	mongodDBPath := backupCursor.BackupCursorMeta.DBPath
	concurrentUploader, err := internal.CreateConcurrentUploader(
		internal.CreateConcurrentUploaderArgs{
			Uploader:             backupService.Uploader,
			BackupName:           backupName,
			Directory:            mongodDBPath,
			TarBallComposerMaker: NewDirDatabaseTarBallComposerMaker(),
		})
	if err != nil {
		return err
	}

	err = concurrentUploader.UploadBackupFiles(backupFiles)
	if err != nil {
		return errors.Wrapf(err, "unable to upload backup files")
	}

	extendBackupCursor, err := conf.GetBoolSettingDefault(conf.MongoDBExtendBackupCursor, true)
	if err != nil {
		return nil
	}

	if extendBackupCursor {
		extendedBackupFiles, err := backupCursor.LoadExtendedBackupCursorFiles()
		if err != nil {
			return errors.Wrapf(err, "unable to load data from backup cursor")
		}

		err = concurrentUploader.UploadBackupFiles(extendedBackupFiles)
		if err != nil {
			return errors.Wrapf(err, "unable to upload backup files")
		}
	}

	tarFileSets, err := concurrentUploader.Finalize()
	if err != nil {
		return err
	}

	if !skipMetadata {
		if err = backupService.AddMetadata(tarFileSets); err != nil {
			tracelog.InfoLogger.Printf("error while uploading metadata, %v", err)
			return err
		}
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

func (backupService *BackupService) Finalize(uploader *internal.ConcurrentUploader, backupCursorMeta *BackupCursorMeta) error {
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

func (backupService *BackupService) AddMetadata(tarFilesSet internal.TarFileSets) error {
	backupRoutes := &backupService.BackupRoutesInfo
	if err := models.EnrichWithTarPaths(backupRoutes, tarFilesSet.Get()); err != nil {
		return err
	}

	return internal.UploadMetadata(backupService.Uploader, backupRoutes, backupService.Sentinel.BackupName)
}
