package binary

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type RestoreService struct {
	Context       context.Context
	LocalStorage  *LocalStorage
	BackupStorage *BackupStorage

	MongodFileConfig *MongodFileConfig
}

func CreateRestoreService(ctx context.Context, localStorage *LocalStorage, backupStorage *BackupStorage,
	mongodFileConfig *MongodFileConfig) (*RestoreService, error) {
	return &RestoreService{
		Context:          ctx,
		LocalStorage:     localStorage,
		BackupStorage:    backupStorage,
		MongodFileConfig: mongodFileConfig,
	}, nil
}

func (restoreService *RestoreService) DoRestore(restoreMongodVersion string) error {
	mongodBackupMeta, err := restoreService.BackupStorage.DownloadMongodBackupMeta()
	if err != nil {
		return err
	}

	err = EnsureCompatibilityToRestoreMongodVersions(restoreMongodVersion, mongodBackupMeta.MongodMeta.Version)
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
	err = restoreService.downloadFilesFromBackup(mongodBackupMeta)
	if err != nil {
		return err
	}

	err = restoreService.fixSystemData(mongodBackupMeta)
	if err != nil {
		return err
	}

	err = restoreService.recoverFromOplogAsStandalone()
	if err != nil {
		return err
	}

	return restoreService.LocalStorage.FixFileOwnerOfMongodData()
}

func (restoreService *RestoreService) fixSystemData(mongodBackupMeta *MongodBackupMeta) error {
	mongodProcess, err := StartMongodWithDisableLogicalSessionCacheRefresh(restoreService.MongodFileConfig)
	if err != nil {
		return errors.Wrap(err, "unable to start mongod in special mode")
	}

	mongodService, err := CreateMongodService(restoreService.Context, "wal-g restore", mongodProcess.GetURI())
	if err != nil {
		return errors.Wrap(err, "unable to create mongod service")
	}

	lastWriteTS := mongodBackupMeta.MongodMeta.EndTS
	err = mongodService.FixSystemDataAfterRestore(lastWriteTS)
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
	mongodProcess, err := StartMongodWithRecoverFromOplogAsStandalone(restoreService.MongodFileConfig)
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

func (restoreService *RestoreService) downloadFilesFromBackup(backupMeta *MongodBackupMeta) error {
	err := restoreService.LocalStorage.EnsureEmptyDBPath()
	if err != nil {
		return err
	}
	err = restoreService.LocalStorage.CreateDirectories(backupMeta.BackupDirectories)
	if err != nil {
		return err
	}

	for _, backupFileMeta := range backupMeta.BackupFiles {
		err = restoreService.DownloadFileFromBackup(backupFileMeta)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("bad backup file %v", backupFileMeta.Path))
		}
	}
	return nil
}

func (restoreService *RestoreService) DownloadFileFromBackup(backupFileMeta *BackupFileMeta) error {
	tracelog.InfoLogger.Printf("copy backup file %s\n", backupFileMeta.Path)

	sourceReader, err := restoreService.BackupStorage.CreateReader(backupFileMeta)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(sourceReader, fmt.Sprintf("backup file reader %v", backupFileMeta.Path))

	return restoreService.LocalStorage.SaveStreamToMongodFile(sourceReader, backupFileMeta)
}
