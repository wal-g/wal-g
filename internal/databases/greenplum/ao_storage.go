package greenplum

import (
	"fmt"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/walparser"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	AoStoragePath       = "aosegments"
	AoSegSuffix         = "_aoseg"
	AoSegDeltaDelimiter = "_D_"
)

func makeAoFileStorageKey(relNameMd5 string, modCount int64, location *walparser.BlockLocation, newAoSegFilesID string) string {
	return fmt.Sprintf("%d_%d_%s_%d_%d_%d_%s%s",
		location.RelationFileNode.SpcNode, location.RelationFileNode.DBNode,
		relNameMd5,
		location.RelationFileNode.RelNode, location.BlockNo,
		modCount, newAoSegFilesID, AoSegSuffix)
}

func makeDeltaAoFileStorageKey(baseKey string, modCount int64) string {
	trimmedKey := strings.TrimSuffix(baseKey, AoSegSuffix)
	return fmt.Sprintf("%s%s%d%s", trimmedKey, AoSegDeltaDelimiter, modCount, AoSegSuffix)
}

// LoadStorageAoFiles loads the list of the AO/AOCS segment files that are referenced from previous backups
func LoadStorageAoFiles(baseBackupsFolder storage.Folder) (map[string]struct{}, error) {
	aoSegments := make(map[string]struct{}, 0)

	iterateFunc := func(_ string, desc BackupAOFileDesc) {
		aoSegments[desc.StoragePath] = struct{}{}
	}
	err := iterateStorageAoFilesWithFunc(baseBackupsFolder, iterateFunc)
	if err != nil {
		return nil, err
	}

	return aoSegments, nil
}

func iterateStorageAoFilesWithFunc(baseBackupsFolder storage.Folder, iterateFunc func(string, BackupAOFileDesc)) error {
	backupObjects, _, err := baseBackupsFolder.ListFolder()
	if err != nil {
		return err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	if err != nil {
		return err
	}

	for _, b := range backupTimes {
		backup, err := NewSegBackup(baseBackupsFolder, b.BackupName)
		if err != nil {
			return err
		}
		aoMeta, err := backup.LoadAoFilesMetadata()
		if err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.WarningLogger.Printf("No AO files metadata found for backup %s in folder %s, skipping",
					backup.Name, baseBackupsFolder.GetPath())
				continue
			}

			return err
		}

		for localPath, fileDesc := range aoMeta.Files {
			iterateFunc(localPath, fileDesc)
		}
	}
	return nil
}
