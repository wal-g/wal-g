package binary

import (
	"context"
	"time"

	conf "github.com/wal-g/wal-g/internal/config"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

type RestoreService struct {
	Context      context.Context
	LocalStorage *LocalStorage
	Uploader     internal.Uploader

	minimalConfigPath string
}

func CreateRestoreService(ctx context.Context, localStorage *LocalStorage, uploader internal.Uploader,
	minimalConfigPath string) (*RestoreService, error) {
	return &RestoreService{
		Context:           ctx,
		LocalStorage:      localStorage,
		Uploader:          uploader,
		minimalConfigPath: minimalConfigPath,
	}, nil
}

func (restoreService *RestoreService) DoRestore(
	rsConfig RsConfig,
	shConfig ShConfig,
	mongoCfgConfig MongoCfgConfig,
	replyOplogConfig ReplyOplogConfig,
	args RestoreArgs,
) error {
	sentinel, err := common.DownloadSentinel(restoreService.Uploader.Folder(), args.BackupName)
	if err != nil {
		return err
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
		if err = restoreService.downloadBackup(args.BackupName, nil); err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped download mongodb backup files")
	}

	if !args.SkipMongoReconfig {
		if err = restoreService.reconfigMongo(
			rsConfig, shConfig, replyOplogConfig,
			mongoCfgConfig, sentinel,
		); err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped mongodb reconfig")
	}

	return nil
}

func (restoreService *RestoreService) downloadBackup(backupName string, tarFilter map[string]struct{}) error {
	err := restoreService.LocalStorage.CleanupMongodDBPath()
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Println("Download backup files to dbPath")
	return restoreService.downloadFromTarArchives(backupName, tarFilter)
}

func (restoreService *RestoreService) doChecks(mongoVersion, restoreVersion string) error {
	err := EnsureCompatibilityToRestoreMongodVersions(mongoVersion, restoreVersion)
	if err != nil {
		return err
	}

	return restoreService.LocalStorage.EnsureMongodFsLockFileIsEmpty()
}

func (restoreService *RestoreService) getFilters(
	metadata *models.BackupRoutesInfo, whitelist, blacklist []string,
) (map[string]struct{}, map[string]struct{}, error) {
	whitelistPathMap := models.PartialWhitelistPathsMap(whitelist)
	blackListPathMap := models.PartialBlacklistPathMap(blacklist)
	return models.GetTarFilesFilter(metadata, whitelistPathMap, blackListPathMap)
}

func (restoreService *RestoreService) reconfigMongo(
	rsConfig RsConfig, shConfig ShConfig, replyOplogConfig ReplyOplogConfig,
	mongoCfgConfig MongoCfgConfig, sentinel *models.Backup,
) error {
	if err := restoreService.fixSystemData(rsConfig, shConfig, mongoCfgConfig); err != nil {
		return err
	}
	if err := restoreService.recoverFromOplogAsStandalone(sentinel); err != nil {
		return err
	}

	if replyOplogConfig.HasPitr {
		if err := restoreService.oplogReply(rsConfig, replyOplogConfig); err != nil {
			return err
		}
	}
	return nil
}

func (restoreService *RestoreService) downloadFromTarArchives(backupName string, filter map[string]struct{}) error {
	downloader := internal.CreateConcurrentDownloader(restoreService.Uploader, restoreService.LocalStorage.whitelist)
	return downloader.Download(backupName, restoreService.LocalStorage.MongodDBPath, filter)
}

func (restoreService *RestoreService) fixSystemData(rsConfig RsConfig, shConfig ShConfig, mongocfgConfig MongoCfgConfig) error {
	mongodProcess, err := StartMongodWithDisableLogicalSessionCacheRefresh(restoreService.minimalConfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}
	defer mongodProcess.Close()

	mongodService, err := CreateMongodService(
		restoreService.Context,
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

	err = mongodService.Shutdown()
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}

func (restoreService *RestoreService) recoverFromOplogAsStandalone(sentinel *models.Backup) error {
	mongodProcess, err := StartMongodWithRecoverFromOplogAsStandalone(restoreService.minimalConfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}

	defer mongodProcess.Close()
	recoverTimeout, err := conf.GetDurationSettingDefault(conf.OplogRecoverTimeout, ComputeMongoStartTimeout(sentinel.UncompressedSize))
	if err != nil {
		return err
	}

	mongodService, err := CreateMongodService(
		restoreService.Context,
		"wal-g restore",
		mongodProcess.GetURI(),
		recoverTimeout,
	)
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	err = mongodService.Shutdown()
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}

func (restoreService *RestoreService) oplogReply(rsConfig RsConfig, replayOplogConfig ReplyOplogConfig) error {
	mongodProcess, err := StartMongodWithReplyOplogAsStandalone(restoreService.minimalConfigPath, rsConfig.RsName, false)
	if err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}

	defer mongodProcess.Close()

	mongodService, err := CreateMongodService(
		restoreService.Context,
		"wal-g restore",
		mongodProcess.GetURI(),
		10*time.Minute,
	)
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	err = RunOplogReplay(restoreService.Context, mongodProcess.GetURI(), replayOplogConfig)
	if err != nil {
		return err
	}

	err = mongodService.Shutdown()
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}

func (restoreService *RestoreService) startMongoWithRestore(sentinel *models.Backup) error {
	mongodProcess, err := StartMongoWithRestore(restoreService.minimalConfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to start mongod in restore mode")
	}
	defer mongodProcess.Close()

	restoreTimeout, err := conf.GetDurationSettingDefault(conf.PartialRestoreTimeout, ComputeMongoStartTimeout(sentinel.UncompressedSize))
	if err != nil {
		return err
	}
	mongodService, err := CreateMongodService(
		restoreService.Context,
		"wal-g restore",
		mongodProcess.GetURI(),
		restoreTimeout,
	)
	if err != nil {
		return err
	}

	if err = mongodService.Shutdown(); err != nil {
		return err
	}

	return mongodProcess.Wait()
}

func (restoreService *RestoreService) PartialRestore(whitelist, blacklist []string, args RestoreArgs) error {
	sentinel, err := common.DownloadSentinel(restoreService.Uploader.Folder(), args.BackupName)
	if err != nil {
		return err
	}

	metadata, err := common.DownloadMetadata(restoreService.Uploader.Folder(), args.BackupName)
	if err != nil {
		return err
	}

	blacklist = append(blacklist, "local.oplog.rs")
	pathFilter, tarFilter, err := restoreService.getFilters(metadata, whitelist, blacklist)
	if err != nil {
		return err
	}

	if !args.SkipChecks {
		if err = restoreService.doChecks(sentinel.MongoMeta.Version, args.RestoreVersion); err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped partial restore mongodb checks")
	}

	if !args.SkipBackupDownload {
		if err = restoreService.downloadBackup(args.BackupName, tarFilter); err != nil {
			return err
		}
	} else {
		tracelog.InfoLogger.Println("Skipped download partial mongodb backup files")
	}

	if err = restoreService.LocalStorage.CleanUpExcessFilesOnPartiallyBackup(pathFilter); err != nil {
		return err
	}

	if !args.SkipMongoReconfig {
		if err = restoreService.startMongoWithRestore(sentinel); err != nil {
			return err
		}
		tracelog.InfoLogger.Println("Started mongo with --restore")
	} else {
		tracelog.InfoLogger.Println("Skipped mongodb reconfig")
	}
	return nil
}
