package ao

import (
	"context"
	"fmt"
	"slices"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// ReassignedSharedSize returns the AO/AOCS size owned by backupName among the surviving backups.
// It reads files metadata only for backupName and its immediately preceding survivor.
func ReassignedSharedSize(ctx context.Context, baseBackupsFolder storage.Folder, backupName string) (int64, error) {
	// Stage 1: get all surviving backups in chronological order. Only their sentinels are read here.
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return 0, err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	internal.SortBackupTimeSlices(backupTimes)
	backupIndex := slices.IndexFunc(backupTimes, func(backup internal.BackupTime) bool {
		return backup.BackupName == backupName
	})
	if backupIndex == -1 {
		return 0, fmt.Errorf("backup %s not found", backupName)
	}

	// Stage 2: load the narrow files metadata view for the affected backup and, if present, its
	// immediately preceding surviving backup.
	files, err := fetchReferencedFiles(ctx, baseBackupsFolder, backupTimes[backupIndex])
	if err != nil {
		return 0, err
	}

	var previousReferences map[string]int64
	if backupIndex > 0 {
		previousReferences, err = fetchReferencedFiles(ctx, baseBackupsFolder, backupTimes[backupIndex-1])
		if err != nil {
			return 0, fmt.Errorf("the preceding backup files metadata is unavailable: %w", err)
		}
	}

	// Stage 3: the affected backup owns every object absent from its current predecessor.
	ownedSize := int64(0)
	for storagePath, size := range files {
		if _, wasReferenced := previousReferences[storagePath]; !wasReferenced {
			ownedSize += size
		}
	}
	return ownedSize, nil
}

func fetchReferencedFiles(ctx context.Context, baseBackupsFolder storage.Folder,
	backupTime internal.BackupTime) (map[string]int64, error) {
	type fileMetadata struct {
		StoragePath string `json:"StoragePath"`
		Size        int64  `json:"EOF"`
	}
	type filesMetadata struct {
		Files map[string]fileMetadata `json:"Files"`
	}

	backup, err := internal.NewBackupInStorage(ctx, baseBackupsFolder, backupTime.BackupName, backupTime.StorageName)
	if err != nil {
		return nil, err
	}

	var meta filesMetadata
	if err := internal.FetchDto(ctx, backup.Folder, &meta, GetFilesMetadataPath(backup.Name)); err != nil {
		return nil, err
	}

	files := make(map[string]int64, len(meta.Files))
	for localPath := range meta.Files {
		desc := meta.Files[localPath]
		files[desc.StoragePath] = desc.Size
	}
	return files, nil
}
