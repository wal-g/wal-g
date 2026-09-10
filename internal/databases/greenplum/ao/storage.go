package ao

import (
	"context"
	"fmt"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	StoragePath    = "aosegments"
	KeySuffix      = "_aoseg"
	DeltaDelimiter = "_D_"
)

func makeFileStorageKey(relNameMd5 string, modCount int64, location *walparser.BlockLocation, newAoSegFilesID string) string {
	return fmt.Sprintf("%d_%d_%s_%d_%d_%d_%s%s",
		location.RelationFileNode.SpcNode, location.RelationFileNode.DBNode,
		relNameMd5,
		location.RelationFileNode.RelNode, location.BlockNo,
		modCount, newAoSegFilesID, KeySuffix)
}

func makeDeltaFileStorageKey(baseKey string, modCount int64) string {
	trimmedKey := strings.TrimSuffix(baseKey, KeySuffix)
	return fmt.Sprintf("%s%s%d%s", trimmedKey, DeltaDelimiter, modCount, KeySuffix)
}

// LoadStorageAOFiles loads the list of the AO/AOCS segment files that are referenced from previous backups.
func LoadStorageAOFiles(ctx context.Context, baseBackupsFolder storage.Folder) (map[string]struct{}, error) {
	aoSegments := make(map[string]struct{}, 0)

	iterateFunc := func(_ string, desc *BackupFileDesc) {
		aoSegments[desc.StoragePath] = struct{}{}
	}
	err := iterateStorageFilesWithFunc(ctx, baseBackupsFolder, iterateFunc)
	if err != nil {
		return nil, err
	}

	return aoSegments, nil
}

func iterateStorageFilesWithFunc(ctx context.Context,
	baseBackupsFolder storage.Folder, iterateFunc func(string, *BackupFileDesc)) error {
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	if err != nil {
		return err
	}

	for _, b := range backupTimes {
		backup, err := internal.NewBackupInStorage(ctx, baseBackupsFolder, b.BackupName, b.StorageName)
		if err != nil {
			return err
		}
		var aoMeta FilesMetadataDTO
		err = internal.FetchDto(ctx, backup.Folder, &aoMeta, GetFilesMetadataPath(backup.Name))
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
