package pgbackrest

import (
	"sort"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type LatestBackupSelector struct {
	Stanza string
}

type NamedBackupSelector struct {
	BackupName string
	Stanza     string
}

func (selector LatestBackupSelector) Select(folder storage.Folder) (internal.Backup, error) {
	backupList, err := GetBackupList(folder, selector.Stanza)
	if err != nil {
		return internal.Backup{}, err
	}
	sort.Slice(backupList, func(i, j int) bool {
		return backupList[i].Time.Before(backupList[j].Time)
	})

	latest := backupList[len(backupList)-1]

	return internal.NewBackupInStorage(folder, latest.BackupName, latest.StorageName)
}

func (selector NamedBackupSelector) Select(folder storage.Folder) (internal.Backup, error) {
	backupList, err := GetBackupList(folder, selector.Stanza)
	if err != nil {
		return internal.Backup{}, err
	}
	for _, backup := range backupList {
		if backup.BackupName == selector.BackupName {
			return internal.NewBackup(folder, backup.BackupName)
		}
	}
	return internal.Backup{}, err
}

func NewBackupSelector(backupName string, stanza string) internal.BackupSelector {
	if backupName == internal.LatestString {
		tracelog.InfoLogger.Printf("Selecting the latest backup...\n")
		return LatestBackupSelector{Stanza: stanza}
	}

	tracelog.InfoLogger.Printf("Selecting the backup with name %s...\n", backupName)
	return NamedBackupSelector{BackupName: backupName, Stanza: stanza}
}
