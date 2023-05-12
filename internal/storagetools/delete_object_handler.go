package storagetools

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleDeleteObject(objectPath string, folder storage.Folder) error {
	// some storages may not produce an error on deleting the non-existing object
	exists, err := folder.Exists(objectPath)
	if err != nil {
		return fmt.Errorf("check object existence: %v", err)
	}

	if !exists {
		return fmt.Errorf("object %s does not exist", objectPath)
	}
	err = folder.DeleteObjects([]string{objectPath})
	if err != nil {
		return fmt.Errorf("delete the specified object: %v", err)
	}
	return nil
}
