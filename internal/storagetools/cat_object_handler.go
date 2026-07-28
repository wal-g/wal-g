package storagetools

import (
	"context"
	"fmt"
	"os"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleCatObject(ctx context.Context, objectPath string, folder storage.Folder, decrypt, decompress bool) error {
	dstFile := os.Stdout
	err := downloadObject(ctx, objectPath, folder, dstFile, decrypt, decompress)
	if err != nil {
		return fmt.Errorf("download the file: %v", err)
	}
	return nil
}

func HandleCatObjectWithGlob(ctx context.Context, pattern string, folder storage.Folder, decrypt, decompress bool) error {
	objectPaths, _, err := storage.Glob(ctx, folder, pattern)
	if err != nil {
		return err
	}
	for _, objectPath := range objectPaths {
		err := HandleCatObject(ctx, objectPath, folder, decrypt, decompress)
		if err != nil {
			return err
		}
	}
	return nil
}
