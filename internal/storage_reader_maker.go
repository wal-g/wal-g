package internal

import (
	"io"
)

// StorageReaderMaker creates readers for downloading from storage
type StorageReaderMaker struct {
	Folder       StorageFolder
	RelativePath string
}

func NewStorageReaderMaker(folder StorageFolder, relativePath string) *StorageReaderMaker {
	return &StorageReaderMaker{folder, relativePath}
}

func (readerMaker *StorageReaderMaker) Path() string { return readerMaker.RelativePath }

func (readerMaker *StorageReaderMaker) Reader() (io.ReadCloser, error) {
	return readerMaker.Folder.ReadObject(readerMaker.RelativePath)

}
