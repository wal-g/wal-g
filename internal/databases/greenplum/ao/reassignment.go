package ao

import (
	"context"
	"fmt"
	"slices"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// ReassignSharedStorage makes the oldest surviving backup that references an AO/AOCS object
// accountable for its size. A following backup owns objects absent from the immediately preceding
// surviving backup.
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

		metadataPath := GetFilesMetadataPath(backup.Name)
		meta, err := fetchFilesMetadata(ctx, backup)
		if err != nil {
			return err
		}
		if meta == nil {
			tracelog.WarningLogger.Printf("No AO/AOCS files metadata found for backup %s in folder %s, skipping",
				backup.Name, baseBackupsFolder.GetPath())
			continue
		}

		var previousReferences map[string]int64
		if backupIndex > 0 {
			previousBackupTime := backupTimes[backupIndex-1]
			previousBackup, err := internal.NewBackupInStorage(ctx, baseBackupsFolder,
				previousBackupTime.BackupName, previousBackupTime.StorageName)
			if err != nil {
				return err
			}

			previousMeta, err := fetchFilesMetadata(ctx, previousBackup)
			if err != nil {
				return err
			}
			if previousMeta == nil {
				tracelog.WarningLogger.Printf("Can not recalculate backup %s shared AO/AOCS size: "+
					"the preceding backup files metadata is unavailable", backup.Name)
				continue
			}
			previousReferences = referencedFiles(previousMeta)
		}

		// Stage 3: compare the affected backup with its predecessor, assign newly referenced objects,
		// and upload the new size when confirmed.
		ownedSize := int64(0)
		for storagePath, size := range referencedFiles(meta) {
			if _, wasReferenced := previousReferences[storagePath]; !wasReferenced {
				ownedSize += size
			}
		}

		if meta.UploadedSharedSize == ownedSize {
			continue
		}

		tracelog.InfoLogger.Printf("Backup %s shared AO/AOCS size changed from %d to %d bytes",
			backup.Name, meta.UploadedSharedSize, ownedSize)
		meta.SetUploadedSharedSize(ownedSize)
		if confirmed {
			if err := internal.UploadDto(ctx, backup.Folder, meta, metadataPath); err != nil {
				return fmt.Errorf("failed to update backup %s shared AO/AOCS size: %w", backup.Name, err)
			}
		}
	}

	return nil
}

func fetchFilesMetadata(ctx context.Context, backup internal.Backup) (*FilesMetadataDTO, error) {
	var meta FilesMetadataDTO
	err := internal.FetchDto(ctx, backup.Folder, &meta, GetFilesMetadataPath(backup.Name))
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); ok {
			return nil, nil
		}
		return nil, err
	}
	return &meta, nil
}

func referencedFiles(meta *FilesMetadataDTO) map[string]int64 {
	files := make(map[string]int64, len(meta.Files))
	for _, desc := range meta.Files {
		if desc == nil {
			continue
		}
		files[desc.StoragePath] = desc.EOF
	}
	return files
}
