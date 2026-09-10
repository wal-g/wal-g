package greenplum

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type sharedFilesKind[T any] struct {
	name            string
	metadataPath    func(backupName string) string
	referencedFiles func(meta *T) map[string]int64
	uploadedSize    func(meta *T) *int64
}

// reassignSharedStorage makes the oldest surviving backup that references a shared object
// accountable for its size. Greenplum segment backup history is linear: references to one
// physical StoragePath form a continuous interval. Therefore a backup owns precisely the objects
// referenced by it, but not by the immediately preceding surviving backup.
//
// The returned set contains every object referenced by a surviving backup and is also used by the
// cleanup pass to find unreferenced objects.
func reassignSharedStorage[T any](ctx context.Context, baseBackupsFolder storage.Folder, confirmed bool,
	kind sharedFilesKind[T]) (map[string]struct{}, error) {
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
		metadataPath := kind.metadataPath(backup.Name)
		if err := internal.FetchDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.WarningLogger.Printf("No %s files metadata found for backup %s in folder %s, skipping",
					kind.name, backup.Name, baseBackupsFolder.GetPath())
				previousMetadataAvailable = false
				continue
			}
			return nil, err
		}

		referencedFiles := kind.referencedFiles(&meta)
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
							size, kind.name, storagePath, backup.Name)
					}
					ownedSize += size
				}
			}

			sizeField := kind.uploadedSize(&meta)
			if *sizeField != ownedSize {
				tracelog.InfoLogger.Printf("Backup %s shared %s size changed from %d to %d bytes",
					backup.Name, kind.name, *sizeField, ownedSize)
				*sizeField = ownedSize
				if confirmed {
					if err := internal.UploadDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
						return nil, fmt.Errorf("failed to update backup %s shared %s size: %w",
							backup.Name, kind.name, err)
					}
				}
			}
		} else {
			tracelog.WarningLogger.Printf("Can not recalculate backup %s shared %s size: "+
				"the preceding backup files metadata is unavailable", backup.Name, kind.name)
		}

		previousReferences = currentReferences
		previousMetadataAvailable = true
	}

	return retained, nil
}

func reassignAOSharedStorage(ctx context.Context, baseBackupsFolder storage.Folder,
	confirmed bool) (map[string]struct{}, error) {
	return reassignSharedStorage(ctx, baseBackupsFolder, confirmed, sharedFilesKind[ao.FilesMetadataDTO]{
		name:         "AO/AOCS",
		metadataPath: ao.GetFilesMetadataPath,
		referencedFiles: func(meta *ao.FilesMetadataDTO) map[string]int64 {
			files := make(map[string]int64, len(meta.Files))
			for _, desc := range meta.Files {
				if desc == nil {
					continue
				}
				files[desc.StoragePath] = desc.EOF
			}
			return files
		},
		uploadedSize: func(meta *ao.FilesMetadataDTO) *int64 { return &meta.UploadedSharedSize },
	})
}

func reassignPaxSharedStorage(ctx context.Context, baseBackupsFolder storage.Folder,
	confirmed bool) (map[string]struct{}, error) {
	return reassignSharedStorage(ctx, baseBackupsFolder, confirmed, sharedFilesKind[pax.FilesMetadataDTO]{
		name:         "PAX",
		metadataPath: pax.GetFilesMetadataPath,
		referencedFiles: func(meta *pax.FilesMetadataDTO) map[string]int64 {
			files := make(map[string]int64, len(meta.Files))
			for _, desc := range meta.Files {
				files[desc.StoragePath] = desc.Size
			}
			return files
		},
		uploadedSize: func(meta *pax.FilesMetadataDTO) *int64 { return &meta.UploadedSharedSize },
	})
}
