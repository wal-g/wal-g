package pax

import (
	"context"
	"fmt"
	"slices"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// ReassignSharedStorage makes the oldest surviving backup that references a PAX object accountable
// for its size. A following backup owns objects absent from the immediately preceding surviving
// backup.
//
//nolint:gocyclo
func ReassignSharedStorage(ctx context.Context, baseBackupsFolder storage.Folder,
	backupsToReassign []string, confirmed bool) error {
	// Stage 1: get all surviving backups in chronological order. Only their sentinels are read here;
	// the cleanup pass is responsible for loading all files metadata and building the retained set.
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	internal.SortBackupTimeSlices(backupTimes)

	for backupIndex, backupTime := range backupTimes {
		if !slices.Contains(backupsToReassign, backupTime.BackupName) {
			continue
		}

		// Stage 2: load files metadata only for the affected backup and its immediately preceding
		// surviving backup.
		backup, err := internal.NewBackupInStorage(ctx, baseBackupsFolder, backupTime.BackupName,
			backupTime.StorageName)
		if err != nil {
			return err
		}

		var meta FilesMetadataDTO
		metadataPath := GetFilesMetadataPath(backup.Name)
		if err := internal.FetchDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.WarningLogger.Printf("No PAX files metadata found for backup %s in folder %s, skipping",
					backup.Name, baseBackupsFolder.GetPath())
				continue
			}
			return err
		}

		var previousReferences map[string]int64
		if backupIndex > 0 {
			previousBackupTime := backupTimes[backupIndex-1]
			previousBackup, err := internal.NewBackupInStorage(ctx, baseBackupsFolder,
				previousBackupTime.BackupName, previousBackupTime.StorageName)
			if err != nil {
				return err
			}

			var previousMeta FilesMetadataDTO
			if err := internal.FetchDto(ctx, previousBackup.Folder, &previousMeta,
				GetFilesMetadataPath(previousBackup.Name)); err != nil {
				if _, ok := err.(storage.ObjectNotFoundError); ok {
					tracelog.WarningLogger.Printf("Can not recalculate backup %s shared PAX size: "+
						"the preceding backup files metadata is unavailable", backup.Name)
					continue
				}
				return err
			}
			previousReferences = referencedFiles(&previousMeta)
		}

		// Stage 3: compare the affected backup with its predecessor, assign newly referenced objects,
		// and upload the new size when confirmed.
		ownedSize := int64(0)
		for storagePath, size := range referencedFiles(&meta) {
			if _, wasReferenced := previousReferences[storagePath]; !wasReferenced {
				ownedSize += size
			}
		}

		if meta.UploadedSharedSize == ownedSize {
			continue
		}

		tracelog.InfoLogger.Printf("Backup %s shared PAX size changed from %d to %d bytes",
			backup.Name, meta.UploadedSharedSize, ownedSize)
		meta.SetUploadedSharedSize(ownedSize)
		if confirmed {
			if err := internal.UploadDto(ctx, backup.Folder, &meta, metadataPath); err != nil {
				return fmt.Errorf("failed to update backup %s shared PAX size: %w", backup.Name, err)
			}
		}
	}

	return nil
}

func referencedFiles(meta *FilesMetadataDTO) map[string]int64 {
	files := make(map[string]int64, len(meta.Files))
	for localPath := range meta.Files {
		desc := meta.Files[localPath]
		files[desc.StoragePath] = desc.Size
	}
	return files
}
