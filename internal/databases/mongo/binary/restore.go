package binary

import (
	"context"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
)

type RestoreService struct {
	Context      context.Context
	LocalStorage *LocalStorage
	Uploader     *internal.Uploader

	minimalConfigPath string
}

func CreateRestoreService(ctx context.Context, localStorage *LocalStorage, uploader *internal.Uploader,
	minimalConfigPath string) (*RestoreService, error) {
	return &RestoreService{
		Context:           ctx,
		LocalStorage:      localStorage,
		Uploader:          uploader,
		minimalConfigPath: minimalConfigPath,
	}, nil
}

func (restoreService *RestoreService) DoRestore(backupName, restoreMongodVersion, rsMembers string) error {
	sentinel, err := common.DownloadSentinel(restoreService.Uploader.Folder(), backupName)
	if err != nil {
		return err
	}

	err = EnsureCompatibilityToRestoreMongodVersions(restoreMongodVersion, sentinel.MongoMeta.Version)
	if err != nil {
		return err
	}

	err = restoreService.LocalStorage.EnsureMongodFsLockFileIsEmpty()
	if err != nil {
		return err
	}

	err = restoreService.LocalStorage.CleanupMongodDBPath()
	if err != nil {
		return err
	}

	tracelog.InfoLogger.Println("Download backup files to dbPath")
	err = restoreService.downloadFromTarArchives(backupName)
	if err != nil {
		return err
	}

	if err = restoreService.fixSystemData(rsMembers); err != nil {
		return err
	}
	if err = restoreService.recoverFromOplogAsStandalone(); err != nil {
		return err
	}

	return nil
}

func (restoreService *RestoreService) downloadFromTarArchives(backupName string) error {
	downloader := CreateConcurrentDownloader(restoreService.Uploader)
	return downloader.Download(backupName, restoreService.LocalStorage.MongodDBPath)
}

func (restoreService *RestoreService) fixSystemData(rsMembers string) error {
	mongodProcess, err := StartMongodWithDisableLogicalSessionCacheRefresh(restoreService.minimalConfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}

	mongodService, err := CreateMongodService(restoreService.Context, "wal-g restore", mongodProcess.GetURI())
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	err = mongodService.FixSystemDataAfterRestore(rsMembers)
	if err != nil {
		return err
	}

	err = mongodService.Shutdown()
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}

func (restoreService *RestoreService) recoverFromOplogAsStandalone() error {
	mongodProcess, err := StartMongodWithRecoverFromOplogAsStandalone(restoreService.minimalConfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}

	mongodService, err := CreateMongodService(restoreService.Context, "wal-g restore", mongodProcess.GetURI())
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	err = mongodService.Shutdown()
	if err != nil {
		return err
	}

	return mongodProcess.Wait()
}
