package ao

import (
	"context"

	"github.com/wal-g/wal-g/internal/databases/greenplum/sharedstorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// ReassignSharedStorage recalculates the AO/AOCS bytes owned by every surviving backup and returns
// all storage objects that remain referenced.
func ReassignSharedStorage(ctx context.Context, baseBackupsFolder storage.Folder,
	confirmed bool) (map[string]struct{}, error) {
	return sharedstorage.Reassign(ctx, baseBackupsFolder, confirmed, sharedstorage.Adapter[FilesMetadataDTO]{
		Name:         "AO/AOCS",
		MetadataPath: GetFilesMetadataPath,
		ReferencedFiles: func(meta *FilesMetadataDTO) map[string]int64 {
			files := make(map[string]int64, len(meta.Files))
			for _, desc := range meta.Files {
				if desc == nil {
					continue
				}
				files[desc.StoragePath] = desc.EOF
			}
			return files
		},
		UploadedSize: func(meta *FilesMetadataDTO) *int64 { return &meta.UploadedSharedSize },
	})
}
