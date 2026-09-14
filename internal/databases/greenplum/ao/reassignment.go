package ao

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// FetchReferencedFiles returns the AO/AOCS storage paths and sizes referenced by backupTime.
// It decodes only StoragePath and EOF from the files metadata.
func FetchReferencedFiles(ctx context.Context, baseBackupsFolder storage.Folder,
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
