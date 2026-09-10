package sharedstorage

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// Adapter describes how one shared storage persists its per-backup metadata.
type Adapter[T any] struct {
	Name            string
	MetadataPath    func(backupName string) string
	ReferencedFiles func(meta *T) map[string]int64
	GetUploadedSize func(meta *T) int64
	SetUploadedSize func(meta *T, size int64)
}

// Reassign makes the oldest surviving backup that references a shared object accountable for its
// size. Greenplum segment backup history is linear: references to one physical storage path form a
// continuous interval. Therefore a backup owns precisely the objects referenced by it, but not by
// the immediately preceding surviving backup.
//
// The returned set contains every object referenced by a surviving backup and is also used by the
// cleanup pass to find unreferenced objects.
func Reassign[T any](ctx context.Context, baseBackupsFolder storage.Folder, confirmed bool,
	adapter Adapter[T]) (map[string]struct{}, error) {
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return nil, err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	internal.SortBackupTimeSlices(backupTimes)

	retained := make(map[string]struct{})
	var previousReferences map[string]struct{}
	previousMetadataAvailable := false

	for backupIndex, backupTime := range backupTimes {
		backup, err := internal.NewBackupInStorage(ctx, baseBackupsFolder, backupTime.BackupName,
			backupTime.StorageName)
		if err != nil {
			return nil, err
		}

		var meta T
		metadataPath := adapter.MetadataPath(backup.Name)
		if err := internal.FetchDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.WarningLogger.Printf("No %s files metadata found for backup %s in folder %s, skipping",
					adapter.Name, backup.Name, baseBackupsFolder.GetPath())
				previousMetadataAvailable = false
				continue
			}
			return nil, err
		}

		referencedFiles := adapter.ReferencedFiles(&meta)
		currentReferences := make(map[string]struct{}, len(referencedFiles))
		for storagePath := range referencedFiles {
			currentReferences[storagePath] = struct{}{}
			retained[storagePath] = struct{}{}
		}

		canCalculate := backupIndex == 0 || previousMetadataAvailable
		if canCalculate {
			ownedSize := int64(0)
			for storagePath, size := range referencedFiles {
				if _, wasReferenced := previousReferences[storagePath]; !wasReferenced {
					if size < 0 {
						return nil, fmt.Errorf("negative size %d for %s object %s in backup %s",
							size, adapter.Name, storagePath, backup.Name)
					}
					ownedSize += size
				}
			}

			previousSize := adapter.GetUploadedSize(&meta)
			if previousSize != ownedSize {
				tracelog.InfoLogger.Printf("Backup %s shared %s size changed from %d to %d bytes",
					backup.Name, adapter.Name, previousSize, ownedSize)
				adapter.SetUploadedSize(&meta, ownedSize)
				if confirmed {
					if err := internal.UploadDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
						return nil, fmt.Errorf("failed to update backup %s shared %s size: %w",
							backup.Name, adapter.Name, err)
					}
				}
			}
		} else {
			tracelog.WarningLogger.Printf("Can not recalculate backup %s shared %s size: "+
				"the preceding backup files metadata is unavailable", backup.Name, adapter.Name)
		}

		previousReferences = currentReferences
		previousMetadataAvailable = true
	}

	return retained, nil
}
