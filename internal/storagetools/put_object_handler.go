package storagetools

import (
	"io"
	"os"
	"path/filepath"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"

	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandlePutObject(localPath, dstPath string, uploader *internal.Uploader, overwrite, encrypt, compress bool) {
	checkOverwrite(dstPath, uploader, overwrite)

	fileReadCloser := openLocalFile(localPath)
	defer fileReadCloser.Close()

	storageFolderPath := utility.SanitizePath(filepath.Dir(dstPath))
	if storageFolderPath != "" {
		folder := uploader.UploadingFolder
		uploader.UploadingFolder = folder.GetSubFolder(storageFolderPath)
	}

	fileName := utility.SanitizePath(filepath.Base(dstPath))
	err := uploadFile(fileName, fileReadCloser, uploader, encrypt, compress)
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

func uploadFile(name string, content io.Reader, uploader *internal.Uploader, encrypt, compress bool) error {
	var crypter crypto.Crypter
	if encrypt {
		crypter = internal.ConfigureCrypter()
	}

	var compressor compression.Compressor
	if compress && uploader.Compressor != nil {
		compressor = uploader.Compressor
		name += "." + uploader.Compressor.FileExtension()
	}

	uploadContents := internal.CompressAndEncrypt(content, compressor, crypter)
	return uploader.Upload(name, uploadContents)
}
