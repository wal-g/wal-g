package internal

import (
	"io"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type StorageFolderReader interface {
	ReadObject(objectRelativePath string) (io.ReadCloser, error)
	SubFolder(subFolderRelativePath string) StorageFolderReader
}

func NewFolderReader(folder storage.Folder) StorageFolderReader {
	return &FolderReaderImpl{folder}
}

type FolderReaderImpl struct {
	storage.Folder
}

func (fsr *FolderReaderImpl) SubFolder(subFolderRelativePath string) StorageFolderReader {
	return NewFolderReader(fsr.GetSubFolder(subFolderRelativePath))
}

func PrepareMultiStorageFolderReader(folder storage.Folder, targetStorage string) (StorageFolderReader, error) {
	folder = multistorage.SetPolicies(folder, policies.MergeAllStorages)
	var err error
	if targetStorage == "" {
		folder, err = multistorage.UseAllAliveStorages(folder)
	} else {
		folder, err = multistorage.UseSpecificStorage(targetStorage, folder)
	}
	tracelog.InfoLogger.Printf("Files will be read from storages: %v", multistorage.UsedStorages(folder))
	if err != nil {
		return nil, err
	}

	return NewFolderReader(folder), nil
}
