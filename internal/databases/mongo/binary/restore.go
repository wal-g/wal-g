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
}

// nolint: whitespace
func CreateRestoreService(
	ctx context.Context,
	localStorage *LocalStorage,
	backupStorage *BackupStorage) (*RestoreService, error) {

	return &RestoreService{
		Context:       ctx,
		LocalStorage:  localStorage,
		BackupStorage: backupStorage,
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
	return restoreService.DownloadFilesFromBackup(mongodBackupMeta)
}

func (restoreService *RestoreService) DownloadFilesFromBackup(backupMeta *MongodBackupMeta) error {
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
