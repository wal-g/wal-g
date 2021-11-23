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

	err = downloadObject(objectPath, folder, dstFile, decrypt, decompress)
	dstFile.Close()
	if err != nil {
		os.Remove(targetPath)
		tracelog.ErrorLogger.Fatalf("Failed to download the file: %v", err)
	}
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
	defer objReadCloser.Close()
	var objReader io.Reader = objReadCloser

	if decrypt {
		objReader, err = internal.DecryptBytes(objReader)
		if err != nil {
			return err
		}
	}

	if decompress {
		fileName := path.Base(objectPath)
		fileExt := path.Ext(fileName)
		decompressor := compression.FindDecompressor(fileExt)
		if decompressor == nil {
			tracelog.WarningLogger.Printf(
				"decompressor for extension '%s' was not found (supported methods: %v), will download uncompressed",
				fileExt, compression.CompressingAlgorithms)
		} else {
			decrypterObjReadCloser, err := decompressor.Decompress(objReader)
			if err != nil {
				return err
			}
			defer decrypterObjReadCloser.Close()
			objReader = decrypterObjReadCloser
		}
	}

	_, err = utility.FastCopy(fileWriter, objReader)
	return err
}
