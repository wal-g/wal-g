package greenplum

import (
	"fmt"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Backup contains information about a valid Greenplum backup
// generated and uploaded by WAL-G.
type Backup struct {
	internal.Backup
	SentinelDto *BackupSentinelDto // used for storage query caching
	rootFolder  storage.Folder
}

func NewBackup(rootFolder storage.Folder, name string) Backup {
	return Backup{
		Backup:     internal.NewBackup(rootFolder.GetSubFolder(utility.BaseBackupPath), name),
		rootFolder: rootFolder,
	}
}

func (backup *Backup) GetSentinel() (BackupSentinelDto, error) {
	if backup.SentinelDto != nil {
		return *backup.SentinelDto, nil
	}
	sentinelDto := BackupSentinelDto{}
	err := backup.FetchSentinel(&sentinelDto)
	if err != nil {
		return sentinelDto, err
	}

	backup.SentinelDto = &sentinelDto
	return sentinelDto, nil
}

func (backup *Backup) GetSegmentBackup(backupID string, contentID int) (SegBackup, error) {
	selector, err := internal.NewUserDataBackupSelector(NewSegmentUserDataFromID(backupID).String(), postgres.NewGenericMetaFetcher())
	if err != nil {
		return SegBackup{}, err
	}
	segBackupsFolder := backup.rootFolder.GetSubFolder(FormatSegmentStoragePrefix(contentID))

	backupName, err := selector.Select(segBackupsFolder)
	if err != nil {
		return SegBackup{}, fmt.Errorf(
			"failed to select matching backup for id %s from subfolder %s: %w",
			backupID, segBackupsFolder.GetPath(), err)
	}

	pgBackup := postgres.NewBackup(segBackupsFolder.GetSubFolder(utility.BaseBackupPath), backupName)
	return ToGpSegBackup(pgBackup), nil
}
