package storagetools

import (
	"io"
	"os"
	"path/filepath"

	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
)

func HandlePutObject(localPath, dstPath string, uploader *internal.Uploader, overwrite bool) {
	checkOverwrite(dstPath, uploader, overwrite)

	fileReadCloser := openLocalFile(localPath)
	defer fileReadCloser.Close()

	storageFolderPath := utility.SanitizePath(filepath.Dir(dstPath))
	if storageFolderPath != "" {
		folder := uploader.UploadingFolder
		uploader.UploadingFolder = folder.GetSubFolder(storageFolderPath)
	}

	fileName := utility.SanitizePath(filepath.Base(dstPath))
	err := uploader.UploadFile(ioextensions.NewNamedReaderImpl(fileReadCloser, fileName))
	tracelog.ErrorLogger.FatalfOnError("Failed to upload: %v", err)
}

func checkOverwrite(dstPath string, uploader *internal.Uploader, overwrite bool) {
	fullPath := dstPath + "." + uploader.Compressor.FileExtension()
	exists, err := uploader.UploadingFolder.Exists(fullPath)
	tracelog.ErrorLogger.FatalfOnError("Failed to check object existence: %v", err)
	if exists && !overwrite {
		tracelog.ErrorLogger.Fatalf("Object %s already exists. To overwrite it, add the -f flag.", fullPath)
	}
}

func openLocalFile(localPath string) io.ReadCloser {
	localFile, err := os.Open(localPath)
	tracelog.ErrorLogger.FatalfOnError("Could not open the local file: %v", err)
	fileInfo, err := localFile.Stat()
	tracelog.ErrorLogger.FatalfOnError("Could not Stat() the local file: %v", err)
	if fileInfo.IsDir() {
		tracelog.ErrorLogger.Fatalf("Provided local path (%s) points to a directory, exiting", localPath)
	}

	return localFile
}
