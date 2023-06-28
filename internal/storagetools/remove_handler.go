package storagetools

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleRemove(prefix string, folder storage.Folder) error {
	objects, err := storage.ListFolderRecursivelyWithPrefix(folder, prefix)
	if err != nil {
		return fmt.Errorf("list files by prefix: %w", err)
	}

	if len(objects) == 0 {
		return fmt.Errorf("object or folder %q does not exist", prefix)
	}

	paths := make([]string, len(objects))
	for i, obj := range objects {
		paths[i] = obj.GetName()
	}

	err = folder.DeleteObjects(paths)
	if err != nil {
		return fmt.Errorf("delete objects by the prefix: %v", err)
	}
	return nil
}
