package pax

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// ReassignSharedStorage makes the oldest surviving backup that references a PAX object accountable
// for its size. A following backup owns objects absent from the immediately preceding surviving
// backup. It returns all storage objects that remain referenced for the cleanup pass.
//
//nolint:gocyclo
func ReassignSharedStorage(ctx context.Context, baseBackupsFolder storage.Folder,
	backupsToReassign []string, confirmed bool) (map[string]struct{}, error) {
	// Stage 1: get all surviving backups in chronological order.
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return nil, err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	internal.SortBackupTimeSlices(backupTimes)

	retained := make(map[string]struct{})
	shouldReassign := make(map[string]struct{}, len(backupsToReassign))
	for _, backupName := range backupsToReassign {
		shouldReassign[backupName] = struct{}{}
	}
	var previousReferences map[string]struct{}
	previousMetadataAvailable := false

	for backupIndex, backupTime := range backupTimes {
		// Stage 2: load the files metadata of the current surviving backup.
		backup, err := internal.NewBackupInStorage(ctx, baseBackupsFolder, backupTime.BackupName,
			backupTime.StorageName)
		if err != nil {
			return nil, err
		}

		var meta FilesMetadataDTO
		metadataPath := GetFilesMetadataPath(backup.Name)
		if err := internal.FetchDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.WarningLogger.Printf("No PAX files metadata found for backup %s in folder %s, skipping",
					backup.Name, baseBackupsFolder.GetPath())
				previousMetadataAvailable = false
				continue
			}
			return nil, err
		}

		// Stage 3: collect the current backup references, compare them with the preceding surviving
		// backup, assign newly referenced objects to the current backup, and upload the new size when
		// confirmed.
		files := referencedFiles(&meta)
		currentReferences := make(map[string]struct{}, len(files))
		for storagePath := range files {
			currentReferences[storagePath] = struct{}{}
			retained[storagePath] = struct{}{}
		}

		_, isAffected := shouldReassign[backup.Name]
		canCalculate := isAffected && (backupIndex == 0 || previousMetadataAvailable)
		if canCalculate {
			ownedSize := int64(0)
			for storagePath, size := range files {
				if _, wasReferenced := previousReferences[storagePath]; !wasReferenced {
					if size < 0 {
						return nil, fmt.Errorf("negative size %d for PAX object %s in backup %s",
							size, storagePath, backup.Name)
					}
					ownedSize += size
				}
			}

			if meta.UploadedSharedSize != ownedSize {
				tracelog.InfoLogger.Printf("Backup %s shared PAX size changed from %d to %d bytes",
					backup.Name, meta.UploadedSharedSize, ownedSize)
				meta.SetUploadedSharedSize(ownedSize)
				if confirmed {
					if err := internal.UploadDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
						return nil, fmt.Errorf("failed to update backup %s shared PAX size: %w",
							backup.Name, err)
					}
				}
			}
		} else if isAffected {
			tracelog.WarningLogger.Printf("Can not recalculate backup %s shared PAX size: "+
				"the preceding backup files metadata is unavailable", backup.Name)
		}

		previousReferences = currentReferences
		previousMetadataAvailable = true
	}

	return retained, nil
}

func referencedFiles(meta *FilesMetadataDTO) map[string]int64 {
	files := make(map[string]int64, len(meta.Files))
	for localPath := range meta.Files {
		desc := meta.Files[localPath]
		files[desc.StoragePath] = desc.Size
	}
	return files
}
