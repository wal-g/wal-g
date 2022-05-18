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

func (selector LatestBackupSelector) Select(folder storage.Folder) (string, error) {
	backupList, err := GetBackupList(folder, selector.Stanza)
	if err != nil {
		return "", err
	}
	sort.Slice(backupList, func(i, j int) bool {
		return backupList[i].Time.Before(backupList[j].Time)
	})

	return backupList[len(backupList)-1].BackupName, nil
}

func (selector NamedBackupSelector) Select(folder storage.Folder) (string, error) {
	backupList, err := GetBackupList(folder, selector.Stanza)
	if err != nil {
		return "", err
	}
	for _, backup := range backupList {
		if backup.BackupName == selector.BackupName {
			return backup.BackupName, nil
		}
	}
	return "", err
}

func NewBackupSelector(backupName string, stanza string) internal.BackupSelector {
	if backupName == internal.LatestString {
		tracelog.InfoLogger.Printf("Selecting the latest backup...\n")
		return LatestBackupSelector{Stanza: stanza}
	}

	tracelog.InfoLogger.Printf("Selecting the backup with name %s...\n", backupName)
	return NamedBackupSelector{BackupName: backupName, Stanza: stanza}
}
