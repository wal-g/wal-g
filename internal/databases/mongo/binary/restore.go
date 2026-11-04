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
	"github.com/wal-g/wal-g/internal/fileparts"
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
		expectedParts := sentinel.FileParts
		if args.IsPartial() {
			expectedParts = filterFileParts(expectedParts, onHostFilesFilter)
		}
		if err = restoreService.downloadBackup(ctx, args.BackupName, tarFilesFilter, expectedParts); err != nil {
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
	backupName string, tarFilter map[string]struct{}, expectedParts *models.FilePartsInfo) error {
	err := restoreService.LocalStorage.CleanupMongodDBPath()
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Println("Download backup files to dbPath")
	return restoreService.downloadFromTarArchives(ctx, backupName, tarFilter, expectedParts)
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
	return runReconfigPhases(reconfigPhases{
		fixReplicationState: func() error {
			return restoreService.fixReplicationState(ctx, rsConfig, partial)
		},
		recoverFromOplog: func() error {
			return restoreService.recoverFromOplogAsStandalone(ctx, sentinel, partial)
		},
		replayOplog: func() error {
			return restoreService.oplogReply(ctx, replyOplogConfig, partial)
		},
		fixShardingMetadata: func() error {
			return restoreService.fixShardingMetadata(ctx, shConfig, mongoCfgConfig, partial)
		},
	}, replyOplogConfig.HasPitr)
}

type reconfigPhases struct {
	fixReplicationState func() error
	recoverFromOplog    func() error
	replayOplog         func() error
	fixShardingMetadata func() error
}

// runReconfigPhases fixes sharding metadata last: oplog entries applied by the recovery and by the PITR replay
// can write the source cluster's shardIdentity, config.shards and config.cache.* documents back.
func runReconfigPhases(phases reconfigPhases, hasPitr bool) error {
	if err := phases.fixReplicationState(); err != nil {
		return err
	}

	if err := phases.recoverFromOplog(); err != nil {
		return err
	}

	if hasPitr {
		if err := phases.replayOplog(); err != nil {
			return err
		}
	}

	return phases.fixShardingMetadata()
}

func (restoreService *RestoreService) downloadFromTarArchives(ctx context.Context,
	backupName string, filter map[string]struct{}, expectedParts *models.FilePartsInfo) error {
	directory := restoreService.LocalStorage.MongodDBPath
	interpreter := fileparts.NewTarInterpreter(directory, internal.NewFileTarInterpreter(directory), expectedParts)
	downloader := internal.CreateConcurrentDownloader(restoreService.Uploader, restoreService.LocalStorage.whitelist)
	return downloader.DownloadWithTarInterpreter(ctx, backupName, directory, filter, interpreter)
}

func (restoreService *RestoreService) fixReplicationState(
	ctx context.Context, rsConfig RsConfig, partial bool,
) error {
	return restoreService.runOnStandaloneMongod(ctx, partial, func(mongodService *MongodService) error {
		if err := mongodService.FixReplset(rsConfig); err != nil {
			return err
		}
		return mongodService.ClearMinvalid()
	})
}

func (restoreService *RestoreService) fixShardingMetadata(
	ctx context.Context, shConfig ShConfig, mongocfgConfig MongoCfgConfig, partial bool,
) error {
	fix := func(mongodService *MongodService) error {
		if err := mongodService.FixShardIdentity(shConfig); err != nil {
			return err
		}
		if err := mongodService.FixMongoCfg(mongocfgConfig); err != nil {
			return err
		}
		return mongodService.CleanCacheAndSessions(shConfig)
	}
	// The data is already at the top of the oplog. Keep the checkpoint unstable, as the recovery and the PITR replay
	// leave it, so that the first replica set start does not apply the oplog over these fixes again.
	return restoreService.runOnStandaloneMongod(ctx, partial, fix, TakeUnstableCheckpointOnShutdown)
}

func (restoreService *RestoreService) runOnStandaloneMongod(
	ctx context.Context, partial bool, fix func(*MongodService) error, extraParams ...string,
) error {
	params := append([]string{DisableLogicalSessionCacheRefresh, SkipShardingConfigurationChecks}, extraParams...)
	mongodProcess := Mongod(restoreService.minimalConfigPath).WithParams(params...)

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

	if err = fix(mongodService); err != nil {
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
	replayOplogConfig.Partial = partial
	replayOplogConfig.MinimalConfigPath = restoreService.minimalConfigPath
	return RunOplogReplay(ctx, "", replayOplogConfig)
}
