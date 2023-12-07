package storagetools

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleMoveObject(srcPath string, dstPath string, folder storage.Folder) error {
	exists, err := folder.Exists(srcPath)

	if !exists {
		return fmt.Errorf("object '%s' does not exist", srcPath)
	}
	if err != nil {
		return err
	}

	// err = folder.MoveObject(srcPath, dstPath)
	if err != nil {
		return fmt.Errorf("move object failed: %w", err)
	}

	return nil
}
