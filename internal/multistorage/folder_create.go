package multistorage

import (
	"context"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (mf Folder) SupportsCreateObject() bool {
	return len(mf.usedFolders) == 1 && storage.SupportsCreateObject(mf.usedFolders[0].Folder)
}

// CreateObject reserves a name on the already selected backup storage. Never
// retry on another storage or treat a name conflict as a storage outage.
func (mf Folder) CreateObject(ctx context.Context, name string, content []byte) error {
	if err := EnsureSingleStorageIsUsed(mf); err != nil {
		return err
	}
	return storage.CreateObject(ctx, mf.usedFolders[0].Folder, name, content)
}
