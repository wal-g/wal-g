package storagetools

import (
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleCatObject(objectPath string, folder storage.Folder, decrypt, decompress bool) {
	dstFile := os.Stdout
	err := downloadObject(objectPath, folder, dstFile, decrypt, decompress)
	tracelog.ErrorLogger.FatalfOnError("Failed to download the file: %v", err)
}
