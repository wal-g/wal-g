package testtools

import (
	"bytes"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type InMemoryStorageObject struct {
	absPath      string
	lastModified time.Time
}

func NewInMemoryStorageObject(absPath string, lastModified time.Time) *InMemoryStorageObject {
	return &InMemoryStorageObject{absPath, lastModified}
}

func (object *InMemoryStorageObject) GetName() string {
	all := strings.SplitAfter(object.absPath, string(os.PathSeparator))
	return all[len(all)-1]
}

func (object *InMemoryStorageObject) GetLastModified() time.Time {
	return object.lastModified
}

type InMemoryStorageFolder struct {
	path    string
	Storage *InMemoryStorage
}

func NewInMemoryStorageFolder(path string, storage *InMemoryStorage) *InMemoryStorageFolder {
	return &InMemoryStorageFolder{path, storage}
}

func MakeDefaultInMemoryStorageFolder() *InMemoryStorageFolder {
	return &InMemoryStorageFolder{"in_memory"+string(os.PathSeparator), NewInMemoryStorage()}
}

func (folder *InMemoryStorageFolder) Exists(objectRelativePath string) (bool, error) {
	_, exists := folder.Storage.Load(filepath.Join(folder.path, objectRelativePath))
	return exists, nil
}

func (folder *InMemoryStorageFolder) GetPath() string {
	return folder.path
}

func (folder *InMemoryStorageFolder) ListFolder() (objects []internal.StorageObject, subFolders []internal.StorageFolder, err error) {
	subFolderNames := sync.Map{}
	folder.Storage.Range(func(key string, value TimeStampedData) bool {
		if !strings.HasPrefix(key, folder.path) {
			return true
		}
		if filepath.Base(key) == strings.TrimPrefix(key, folder.path) {
			objects = append(objects, NewInMemoryStorageObject(key, value.Timestamp))
		} else {
			subFolderName := strings.Split(strings.TrimPrefix(key, folder.path), string(os.PathSeparator))[0]
			subFolderNames.Store(subFolderName, true)
		}
		return true
	})
	subFolderNames.Range(func(iName, _ interface{}) bool {
		name := iName.(string)
		subFolders = append(subFolders, NewInMemoryStorageFolder(filepath.Join(folder.path,name)+string(os.PathSeparator), folder.Storage))
		return true
	})
	return
}

func (folder *InMemoryStorageFolder) DeleteObjects(objectRelativePaths []string) error {
	panic("implement me")
}

func (folder *InMemoryStorageFolder) GetSubFolder(subFolderRelativePath string) internal.StorageFolder {
	return NewInMemoryStorageFolder(filepath.Join(folder.path,subFolderRelativePath)+string(os.PathSeparator), folder.Storage)
}

func (folder *InMemoryStorageFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectAbsPath := filepath.Join(folder.path, objectRelativePath)
	object, exists := folder.Storage.Load(objectAbsPath)
	if !exists {
		return nil, internal.NewObjectNotFoundError(objectAbsPath)
	}
	return ioutil.NopCloser(&object.Data), nil
}

func (folder *InMemoryStorageFolder) PutObject(name string, content io.Reader) error {
	data, err := ioutil.ReadAll(content)
	objectPath := filepath.Join(folder.path, name)
	if err != nil {
		return errors.Wrapf(err, "failed to put '%s' in memory storage", objectPath)
	}
	folder.Storage.Store(objectPath, *bytes.NewBuffer(data))
	return nil
}
