package internal

import (
	"io"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// StorageReaderMaker creates readers for downloading from storage
type StorageReaderMaker struct {
	Folder          storage.Folder
	storagePath     string
	localPath       string
	StorageFileType FileType
	FileMode        int64
}

func NewStorageReaderMaker(folder storage.Folder, relativePath string) *StorageReaderMaker {
	return &StorageReaderMaker{folder, relativePath, relativePath, TarFileType, 0}
}

func NewRegularFileStorageReaderMarker(folder storage.Folder, storagePath, localPath string, fileMode int64) *StorageReaderMaker {
	return &StorageReaderMaker{folder, storagePath, localPath, RegularFileType, fileMode}
}

func (readerMaker *StorageReaderMaker) StoragePath() string { return readerMaker.storagePath }

func (readerMaker *StorageReaderMaker) LocalPath() string { return readerMaker.localPath }

func (readerMaker *StorageReaderMaker) Reader() (io.ReadCloser, error) {
	return readerMaker.Folder.ReadObject(readerMaker.storagePath)
}

func (readerMaker *StorageReaderMaker) FileType() FileType { return readerMaker.StorageFileType }

func (readerMaker *StorageReaderMaker) Mode() int64 { return readerMaker.FileMode }
