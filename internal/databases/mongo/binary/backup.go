package binary

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/wal-g/wal-g/internal/databases/mongo/partial"

	"github.com/wal-g/tracelog"

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

type CalculateSizesArgs struct {
	BackupName    string
	CountJournals bool
}

func (backupService *BackupService) createInitialJournals(journalFiles *internal.JournalFiles) internal.JournalInfo {
	backupFolder, err := common.GetBackupFolder()
	if err != nil {
		tracelog.ErrorLogger.Printf("can not get backup folder: %+v", err)
		return internal.JournalInfo{}
	}
	backupTimes, err := internal.GetBackups(backupFolder)
	if err != nil {
		// no backups is a valid case, no journals should be created then
		slog.Warn(fmt.Sprintf("can not get backups: %+v", err))
		return internal.JournalInfo{}
	}

	slog.Warn(fmt.Sprintf("trying to create initial journals"))
	internal.SortBackupTimeSlices(backupTimes)
	mostRecentJournalInfo := internal.JournalInfo{}
	for _, backupTime := range backupTimes {
		mostRecentJournalInfo = backupService.addJournalInfo(addJournalInfoArgs{
			backupName:            backupTime.BackupName,
			mostRecentJournalInfo: mostRecentJournalInfo,
			timeStop:              backupTime.Time,
			journalFiles:          journalFiles,
		})
	}
	return mostRecentJournalInfo
}

type addJournalInfoArgs struct {
	backupName            string
	mostRecentJournalInfo internal.JournalInfo
	timeStop              time.Time
	journalFiles          *internal.JournalFiles
}

func (backupService *BackupService) addJournalInfo(args addJournalInfoArgs) internal.JournalInfo {
	if !strings.HasPrefix(args.backupName, common.BinaryBackupType) {
		return args.mostRecentJournalInfo
	}

	storage, err := internal.ConfigureStorage()
	if err != nil {
		slog.Warn(fmt.Sprintf("Can't configure storage: %+v", err))
		return internal.JournalInfo{}
	}

	rootFolder := storage.RootFolder()
	journalInfo := internal.NewEmptyJournalInfo(
		args.backupName,
		args.mostRecentJournalInfo.CurrentBackupEnd, args.timeStop,
		models.OplogArchBasePath,
	)

	err = journalInfo.Upload(rootFolder)
	if err != nil {
		slog.Warn(fmt.Sprintf("can not upload the journal info: %+v", err))
		return internal.JournalInfo{}
	}

	err = journalInfo.UpdateIntervalSize(rootFolder, args.journalFiles)
	if err != nil {
		slog.Warn(fmt.Sprintf("can not calculate journal size: %+v", err))
		return internal.JournalInfo{}
	}

	slog.Info(fmt.Sprintf("uploaded journal info for %s", args.backupName))
	return journalInfo
}

func (backupService *BackupService) calculateSizes(args CalculateSizesArgs) {
	if !args.CountJournals {
		slog.Info(fmt.Sprintf("oplog counting mode is disabled: option is disabled"))
		return
	}

	storage, err := internal.ConfigureStorage()
	if err != nil {
		slog.Warn(fmt.Sprintf("Can't configure storage: %+v", err))
		return
	}

	journalFiles := &internal.JournalFiles{}
	mostRecentJournalInfo, err := internal.GetMostRecentJournalInfo(
		storage.RootFolder(),
		models.OplogArchBasePath,
	)
	if errors.Is(err, internal.JournalsNotFound) {
		// there can be no backups on S3 or we do it first time
		slog.Warn(fmt.Sprintf("can not find the last journal info: %+v", err))
		mostRecentJournalInfo = backupService.createInitialJournals(journalFiles)
	}

	timeStop := utility.TimeNowCrossPlatformLocal()
	backupService.addJournalInfo(addJournalInfoArgs{
		backupName:            args.BackupName,
		mostRecentJournalInfo: mostRecentJournalInfo,
		timeStop:              timeStop,
		journalFiles:          journalFiles,
	})
}

type DoBackupArgs struct {
	BackupName    string
	CountJournals bool
	Permanent     bool
	SkipMetadata  bool
}

func (backupService *BackupService) DoBackup(args DoBackupArgs) error {
	err := backupService.InitializeMongodBackupMeta(args.BackupName, args.Permanent)
	if err != nil {
		return err
	}

	tarsChan := make(chan internal.TarFileSets)
	errsChan := make(chan error)
	go backupService.BackgroundMetadata(tarsChan, errsChan, args.SkipMetadata)

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
			BackupName:           args.BackupName,
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

	if !args.SkipMetadata {
		tarsChan <- tarFileSets
		err = <-errsChan
		if err != nil {
			return err
		}
	}

	return backupService.Finalize(concurrentUploader, backupCursor.BackupCursorMeta, args.CountJournals)
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

func (backupService *BackupService) Finalize(uploader *internal.ConcurrentUploader, backupCursorMeta *BackupCursorMeta,
	countJournals bool) error {
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

	err := internal.UploadSentinel(backupService.Uploader, sentinel, sentinel.BackupName)
	if err != nil {
		return err
	}

	calculateArgs := CalculateSizesArgs{
		BackupName:    sentinel.BackupName,
		CountJournals: countJournals,
	}
	backupService.calculateSizes(calculateArgs)
	return nil
}

func (backupService *BackupService) BackgroundMetadata(
	tarsChan <-chan internal.TarFileSets,
	errChan chan<- error,
	skip bool,
) {
	if skip {
		errChan <- nil
		return
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mongodbURI, err := conf.GetRequiredSetting(conf.MongoDBUriSetting)
	if err != nil {
		errChan <- err
		return
	}

	bgMongoService, err := CreateBackgroundMongodService(bgCtx, "bg-metadata", mongodbURI)
	if err != nil {
		errChan <- err
		return
	}

	backupRoutes, err := CreateBackupRoutesInfo(bgMongoService)
	if err != nil {
		errChan <- err
		return
	}

	tarsFileSet := <-tarsChan
	if err = partial.EnrichWithTarPaths(backupRoutes, tarsFileSet.Get()); err != nil {
		errChan <- err
		return
	}

	errChan <- internal.UploadMetadata(backupService.Uploader, backupRoutes, backupService.Sentinel.BackupName)
}
