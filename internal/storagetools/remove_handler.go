package storagetools

import (
	"context"
	"fmt"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleRemove(ctx context.Context, prefix string, folder storage.Folder) error {
	objects, err := storage.ListFolderRecursivelyWithPrefix(ctx, folder, prefix)
	if err != nil {
		return fmt.Errorf("list files by prefix: %w", err)
	}

	if len(objects) == 0 {
		return fmt.Errorf("object or folder %q does not exist", prefix)
	}

	err = folder.DeleteObjects(ctx, objects)
	if err != nil {
		return fmt.Errorf("delete objects by the prefix: %v", err)
	}
	return nil
}

func HandleRemoveWithGlobPattern(ctx context.Context, pattern string, folder storage.Folder) error {
	objectPaths, folderPaths, err := storage.Glob(ctx, folder, pattern)
	if err != nil {
		return err
	}
	for _, objectPath := range objectPaths {
		err := HandleRemove(ctx, objectPath, folder)
		if err != nil {
			return err
		}
	}

	for _, folderPath := range folderPaths {
		err := HandleRemove(ctx, folderPath, folder)
		if err != nil {
			return err
		}
	}
	return nil
}

func HandleRemoveVersion(ctx context.Context, key string, versionID string, folder storage.Folder) error {
	obj := storage.NewLocalObjectWithVersion(key, time.Time{}, 0, versionID, "")
	err := folder.DeleteObjects(ctx, []storage.Object{obj})
	if err != nil {
		return fmt.Errorf("delete object %q version %q: %w", key, versionID, err)
	}
	return nil
}
