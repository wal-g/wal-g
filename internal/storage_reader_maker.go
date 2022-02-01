package internal

import (
	"io"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// StorageReaderMaker creates readers for downloading from storage
type StorageReaderMaker struct {
	Folder          storage.Folder
	RelativePath    string
	StorageFileType FileType
	FileMode        int
}

func NewStorageReaderMaker(folder storage.Folder, relativePath string) *StorageReaderMaker {
	return &StorageReaderMaker{folder, relativePath, TarFileType, 0}
}

func NewRegularFileStorageReaderMarker(folder storage.Folder, relativePath string, fileMode int) *StorageReaderMaker {
	return &StorageReaderMaker{folder, relativePath, RegularFileType, fileMode}
}

func (readerMaker *StorageReaderMaker) Path() string { return readerMaker.RelativePath }

func (readerMaker *StorageReaderMaker) Reader() (io.ReadCloser, error) {
	return readerMaker.Folder.ReadObject(readerMaker.RelativePath)
}

func (readerMaker *StorageReaderMaker) FileType() FileType { return readerMaker.StorageFileType }

func (readerMaker *StorageReaderMaker) Mode() int { return readerMaker.FileMode }
