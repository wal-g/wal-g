package testtools

import (
	"bytes"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g"
	"io"
	"io/ioutil"
)

type InMemoryStorageFolder struct {
	path string
	storage *InMemoryStorage
}

func NewInMemoryStorageFolder(path string, storage *InMemoryStorage) *InMemoryStorageFolder {
	return &InMemoryStorageFolder{path, storage}
}

func (folder *InMemoryStorageFolder) Exists(objectRelativePath string) (bool, error) {
	panic("implement me")
}

func (folder *InMemoryStorageFolder) GetPath() string {
	return folder.path
}

func (folder *InMemoryStorageFolder) ListFolder() (objects []walg.StorageObject, subFolders []walg.StorageFolder, err error) {
	panic("implement me")
}

func (folder *InMemoryStorageFolder) DeleteObjects(objectRelativePaths []string) error {
	panic("implement me")
}

func (folder *InMemoryStorageFolder) GetSubFolder(subFolderRelativePath string) walg.StorageFolder {
	return NewInMemoryStorageFolder(folder.path + subFolderRelativePath, folder.storage)
}

func (folder *InMemoryStorageFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectAbsPath := folder.path + objectRelativePath
	object, exists := folder.storage.Load(objectAbsPath)
	if !exists {
		return nil, walg.NewObjectNotFoundError(objectAbsPath)
	}
	return ioutil.NopCloser(&object), nil
}

func (folder *InMemoryStorageFolder) PutObject(name string, content io.Reader) error {
	data, err := ioutil.ReadAll(content)
	objectPath := folder.path + name
	if err != nil {
		return errors.Wrapf(err, "failed to put '%s' in memory storage", objectPath)
	}
	folder.storage.Store(objectPath, *bytes.NewBuffer(data))
	return nil
}
