package postgres

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/walparser"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	AoStoragePath = "aosegments"
	AoSegSuffix   = "_aoseg"
)

func makeAoFileStorageKey(relNameMd5 string, modCount int64, location *walparser.BlockLocation) string {
	return fmt.Sprintf("%d_%d_%s_%d_%d_%d%s",
		location.RelationFileNode.SpcNode, location.RelationFileNode.DBNode,
		relNameMd5,
		location.RelationFileNode.RelNode, location.BlockNo,
		modCount, AoSegSuffix)
}

//LoadStorageAoFiles loads the list of the AO/AOCS segment files that are referenced from previous backups
func LoadStorageAoFiles(baseBackupsFolder storage.Folder) (map[string]struct{}, error) {
	backupObjects, _, err := baseBackupsFolder.ListFolder()
	if err != nil {
		return nil, err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	if err != nil {
		return nil, err
	}

	aoSegments := make(map[string]struct{}, 0)
	for _, b := range backupTimes {
		backup := NewBackup(baseBackupsFolder, b.BackupName)
		aoMeta, err := backup.LoadAoFilesMetadata()
		if err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.WarningLogger.Printf("No AO files metadata found for backup %s in folder %s, skipping",
					backup.Name, baseBackupsFolder.GetPath())
				continue
			}

			return nil, err
		}

		for _, fileDesc := range aoMeta.Files {
			aoSegments[fileDesc.StoragePath] = struct{}{}
		}
	}

	return aoSegments, nil
}
