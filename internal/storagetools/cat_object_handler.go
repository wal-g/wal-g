package storagetools

import (
	"fmt"
	"os"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleCatObject(objectPath string, folder storage.Folder, decrypt, decompress bool) error {
	dstFile := os.Stdout
	err := downloadObject(objectPath, folder, dstFile, decrypt, decompress)
	if err != nil {
		return fmt.Errorf("download the file: %v", err)
	}
	return nil
}
