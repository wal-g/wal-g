package storagetools

import (
	"errors"
	"io"
	"os"
	"path"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func HandleGetObject(objectPath, dstPath string, folder storage.Folder, decrypt, decompress bool) {
	fileName := path.Base(objectPath)
	targetPath, err := getTargetFilePath(dstPath, fileName)
	tracelog.ErrorLogger.FatalfOnError("Failed to determine the destination path: %v", err)

	dstFile, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0640)
	tracelog.ErrorLogger.FatalfOnError("Failed to open the destination file: %v", err)
	defer dstFile.Close()

	err = downloadObject(objectPath, folder, dstFile, decrypt, decompress)
	tracelog.ErrorLogger.FatalfOnError("Failed to download the file: %v", err)
}

func getTargetFilePath(dstPath string, fileName string) (string, error) {
	info, err := os.Stat(dstPath)
	if errors.Is(err, os.ErrNotExist) {
		return dstPath, nil
	}

	if err != nil {
		return "", err
	}

	if info.IsDir() {
		return path.Join(dstPath, fileName), nil
	}

	return dstPath, nil
}

func downloadObject(objectPath string, folder storage.Folder, fileWriter io.Writer, decrypt, decompress bool) error {
	objReadCloser, err := folder.ReadObject(objectPath)
	if err != nil {
		return err
	}
	origReadCloser := objReadCloser
	defer origReadCloser.Close()

	if decrypt {
		objReadCloser, err = internal.DecryptBytes(objReadCloser)
		if err != nil {
			return err
		}
	}

	if decompress {
		fileName := path.Base(objectPath)
		fileExt := path.Ext(fileName)
		decompressor := compression.FindDecompressor(fileExt)
		if decompressor != nil {
			return decompressor.Decompress(fileWriter, objReadCloser)
		}

		tracelog.WarningLogger.Printf(
			"decompressor for extension '%s' was not found (supported methods: %v), will download uncompressed",
			fileExt, compression.CompressingAlgorithms)
	}

	_, err = utility.FastCopy(fileWriter, objReadCloser)
	return err
}
