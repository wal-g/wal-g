package storagetools

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"

	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal"
)

func HandlePutObject(source io.Reader, dstPath string, uploader internal.Uploader, overwrite, encrypt, compress bool) error {
	err := checkOverwrite(dstPath, uploader, overwrite)
	if err != nil {
		return fmt.Errorf("check file overwrite: %v", err)
	}

	storageFolderPath := utility.SanitizePath(filepath.Dir(dstPath))
	if storageFolderPath != "" {
		uploader.ChangeDirectory(storageFolderPath)
	}

	fileName := utility.SanitizePath(filepath.Base(dstPath))
	err = uploadFile(fileName, source, uploader, encrypt, compress)
	if err != nil {
		return fmt.Errorf("upload: %v", err)
	}
	return nil
}

func checkOverwrite(dstPath string, uploader internal.Uploader, overwrite bool) error {
	fullPath := dstPath + "." + uploader.Compression().FileExtension()
	exists, err := uploader.Folder().Exists(fullPath)
	if err != nil {
		return fmt.Errorf("check object existence: %v", err)
	}
	if exists && !overwrite {
		return fmt.Errorf("object %s already exists. To overwrite it, add the -f flag", fullPath)
	}
	return nil
}

func uploadFile(name string, content io.Reader, uploader internal.Uploader, encrypt, compress bool) error {
	var crypter crypto.Crypter
	if encrypt {
		crypter = internal.ConfigureCrypter()
	}

	var compressor compression.Compressor
	if compress && uploader.Compression() != nil {
		compressor = uploader.Compression()
		name += "." + uploader.Compression().FileExtension()
	}

	uploadContents := internal.CompressAndEncrypt(content, compressor, crypter)
	return uploader.Upload(name, uploadContents)
}
