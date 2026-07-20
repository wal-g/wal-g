package binary

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

var (
	DisableLogicalSessionCacheRefresh = "disableLogicalSessionCacheRefresh=true"
	SkipShardingConfigurationChecks   = "skipShardingConfigurationChecks=true"
	RecoverFromOplogAsStandalone      = "recoverFromOplogAsStandalone=true"
	TakeUnstableCheckpointOnShutdown  = "takeUnstableCheckpointOnShutdown=true"
)

type RestoreService struct {
	LocalStorage *LocalStorage
	Uploader     internal.Uploader

	minimalConfigPath string
}

func CreateRestoreService(localStorage *LocalStorage, uploader internal.Uploader,
	minimalConfigPath string) (*RestoreService, error) {
	return &RestoreService{
		LocalStorage:      localStorage,
		Uploader:          uploader,
		minimalConfigPath: minimalConfigPath,
	}, nil
}

func (restoreService *RestoreService) DoRestore(
	ctx context.Context,
	rsConfig RsConfig,
	shConfig ShConfig,
	mongoCfgConfig MongoCfgConfig,
	replyOplogConfig ReplyOplogConfig,
	args RestoreArgs,
) error {
	sentinel, err := common.DownloadSentinel(ctx, restoreService.Uploader.Folder(), args.BackupName)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Sentinel %v", sentinel)

	var onHostFilesFilter, tarFilesFilter map[string]struct{}

	if args.IsPartial() {
		metadata, err := common.DownloadMetadata(ctx, restoreService.Uploader.Folder(), args.BackupName)
		if err != nil {
			return err
		}
		onHostFilesFilter, tarFilesFilter = GetTarFilesFilter(metadata, args.Whitelist, args.Blacklist)
	}

	if !args.SkipChecks {
		//todo maybe delete all checks?
		if err = restoreService.doChecks(sentinel.MongoMeta.Version, args.RestoreVersion); err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped restore mongodb checks")
	}

	if !args.SkipBackupDownload {
		if err = restoreService.downloadBackup(ctx, args.BackupName, tarFilesFilter); err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped download mongodb backup files")
	}

	if args.IsPartial() {
		if err = restoreService.LocalStorage.CleanUpExcessFilesOnPartiallyBackup(onHostFilesFilter); err != nil {
			return err
		}
	}

	if !args.SkipMongoReconfig {
		if err = restoreService.reconfigMongo(
			ctx, rsConfig, shConfig, replyOplogConfig,
			mongoCfgConfig, sentinel, args.IsPartial(),
		); err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped mongodb reconfig")
	}
	return nil
}

func (restoreService *RestoreService) downloadBackup(ctx context.Context,
	backupName string, tarFilter map[string]struct{}) error {
	err := restoreService.LocalStorage.CleanupMongodDBPath()
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Println("Download backup files to dbPath")
	return restoreService.downloadFromTarArchives(ctx, backupName, tarFilter)
}

func (restoreService *RestoreService) doChecks(mongoVersion, restoreVersion string) error {
	err := EnsureCompatibilityToRestoreMongodVersions(mongoVersion, restoreVersion)
	if err != nil {
		return err
	}

	return restoreService.LocalStorage.EnsureMongodFsLockFileIsEmpty()
}

func (restoreService *RestoreService) reconfigMongo(
	ctx context.Context,
	rsConfig RsConfig, shConfig ShConfig, replyOplogConfig ReplyOplogConfig,
	mongoCfgConfig MongoCfgConfig, sentinel *models.Backup, partial bool,
) error {
	if err := restoreService.fixSystemData(ctx, rsConfig, shConfig, mongoCfgConfig, partial); err != nil {
		return err
	}

	if err := restoreService.recoverFromOplogAsStandalone(ctx, sentinel, partial); err != nil {
		return err
	}

	if replyOplogConfig.HasPitr {
		if err := restoreService.oplogReply(ctx, replyOplogConfig, partial); err != nil {
			return err
		}
	}
	return nil
}

func (restoreService *RestoreService) downloadFromTarArchives(ctx context.Context,
	backupName string, filter map[string]struct{}) error {
	downloader := internal.CreateConcurrentDownloader(restoreService.Uploader, restoreService.LocalStorage.whitelist)
	return downloader.Download(ctx, backupName, restoreService.LocalStorage.MongodDBPath, filter)
}

func (restoreService *RestoreService) fixSystemData(
	ctx context.Context,
	rsConfig RsConfig, shConfig ShConfig, mongocfgConfig MongoCfgConfig, partial bool,
) error {
	mongodProcess := Mongod(restoreService.minimalConfigPath).
		WithParams(DisableLogicalSessionCacheRefresh, SkipShardingConfigurationChecks)

	if partial {
		mongodProcess.WithRestore()
	}

	if _, err := mongodProcess.Start(ctx); err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}
	defer mongodProcess.Close()

	mongodService, err := CreateMongodService(
		ctx,
		"wal-g restore",
		mongodProcess.GetURI(),
		10*time.Minute,
	)
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	if err = mongodService.FixReplset(rsConfig); err != nil {
		return err
	}

	if err = mongodService.FixShardIdentity(shConfig); err != nil {
		return err
	}

	if err = mongodService.FixMongoCfg(mongocfgConfig); err != nil {
		return err
	}

	if err = mongodService.CleanCacheAndSessions(shConfig); err != nil {
		return err
	}

	if err = mongodService.ClearMinvalid(); err != nil {
		return err
	}

	err = mongodService.Shutdown(ctx)
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}

func (restoreService *RestoreService) recoverFromOplogAsStandalone(ctx context.Context,
	sentinel *models.Backup, partial bool) error {
	mongodProcess := Mongod(restoreService.minimalConfigPath).
		WithParams(RecoverFromOplogAsStandalone, TakeUnstableCheckpointOnShutdown)

	if partial {
		mongodProcess.WithRestore()
	}

	if _, err := mongodProcess.Start(ctx); err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}

	defer mongodProcess.Close()
	recoverTimeout, err := conf.GetDurationSettingDefault(conf.OplogRecoverTimeout, ComputeMongoStartTimeout(sentinel.UncompressedSize))
	if err != nil {
		return err
	}

	mongodService, err := CreateMongodService(
		ctx,
		"wal-g restore",
		mongodProcess.GetURI(),
		recoverTimeout,
	)
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	err = mongodService.Shutdown(ctx)
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}

func (restoreService *RestoreService) oplogReply(ctx context.Context,
	replayOplogConfig ReplyOplogConfig, partial bool) error {
	mongodProcess := Mongod(restoreService.minimalConfigPath).
		WithParams(DisableLogicalSessionCacheRefresh, TakeUnstableCheckpointOnShutdown)

	if partial {
		mongodProcess.WithRestore()
	}

	if _, err := mongodProcess.Start(ctx); err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}

	defer mongodProcess.Close()

	mongodService, err := CreateMongodService(
		ctx,
		"wal-g restore",
		mongodProcess.GetURI(),
		10*time.Minute,
	)
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	err = RunOplogReplay(ctx, mongodProcess.GetURI(), replayOplogConfig)
	if err != nil {
		return err
	}

	err = mongodService.Shutdown(ctx)
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}
