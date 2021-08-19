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

type DownloadMode string

const (
	DownloadRaw        DownloadMode = "raw"
	DownloadDecompress DownloadMode = "decompress"
	DownloadDecrypt    DownloadMode = "decrypt"
)

func HandleGetObject(objectPath, dstPath string, folder storage.Folder, mode DownloadMode) {
	fileName := path.Base(objectPath)
	targetPath, err := getTargetFilePath(dstPath, fileName)
	tracelog.ErrorLogger.FatalfOnError("Failed to determine the destination path: %v", err)

	dstFile, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
	tracelog.ErrorLogger.FatalfOnError("Failed to open the destination file: %v", err)
	defer dstFile.Close()

	err = downloadObject(objectPath, folder, mode, dstFile)
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

func downloadObject(objectPath string, folder storage.Folder, mode DownloadMode, fileWriter io.WriteCloser) error {
	switch mode {
	case DownloadDecrypt:
		return decryptDownload(objectPath, folder, fileWriter)

	case DownloadDecompress:
		return decompressDownload(objectPath, folder, fileWriter)

	default:
		return rawDownload(objectPath, folder, fileWriter)
	}
}

func rawDownload(objectPath string, folder storage.Folder, dstWriter io.Writer) error {
	fileReadCloser, err := folder.ReadObject(objectPath)
	if err != nil {
		return err
	}
	defer fileReadCloser.Close()

	_, err = utility.FastCopy(dstWriter, fileReadCloser)
	return err
}

func decompressDownload(objectPath string, folder storage.Folder, dstWriter io.WriteCloser) error {
	fileName := path.Base(objectPath)
	fileExt := path.Ext(fileName)

	decompressor := compression.FindDecompressor(fileExt)
	if decompressor == nil {
		tracelog.WarningLogger.Printf(
			"decompressor for extension '%s' was not found, will download uncompressed", fileExt)
		return rawDownload(objectPath, folder, dstWriter)
	}

	return internal.DownloadFile(folder, objectPath, fileExt, dstWriter)
}

func decryptDownload(objectPath string, folder storage.Folder, dstWriter io.Writer) error {
	rawFileReadCloser, err := folder.ReadObject(objectPath)
	if err != nil {
		return err
	}
	defer rawFileReadCloser.Close()

	fileReadCloser, err := internal.DecryptBytes(rawFileReadCloser)
	if err != nil {
		return err
	}
	_, err = utility.FastCopy(dstWriter, fileReadCloser)
	return err
}
