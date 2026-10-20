package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (folder *Folder) CreateObject(ctx context.Context, name string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filePath := folder.GetFilePath(name)
	if err := os.MkdirAll(filepath.Dir(filePath), dirDefaultMode); err != nil {
		return err
	}
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		return fmt.Errorf("create %q: %w", name, storage.ErrObjectAlreadyExists)
	}
	if err != nil {
		return err
	}
	// Even an empty/partially written reservation must remain after a failure.
	defer file.Close()
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return nil // Same directory durability limitation as PutObject.
	}
	dir, err := os.Open(filepath.Dir(filePath))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
