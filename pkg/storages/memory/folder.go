package memory

import (
	"bytes"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type Folder struct {
	path    string
	Storage *Storage
}

func NewFolder(path string, storage *Storage) *Folder {
	return &Folder{path, storage}
}

func NewError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "Memory", format, args...)
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	_, exists := folder.Storage.Load(path.Join(folder.path, objectRelativePath))
	return exists, nil
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	subFolderNames := sync.Map{}
	folder.Storage.Range(func(key string, value TimeStampedData) bool {
		if !strings.HasPrefix(key, folder.path) {
			return true
		}
		if filepath.Base(key) == strings.TrimPrefix(key, folder.path) {
			nameParts := strings.SplitAfter(key, "/")
			objects = append(objects, storage.NewLocalObject(nameParts[len(nameParts)-1], value.Timestamp, int64(value.Size)))
		} else {
			subFolderName := strings.Split(strings.TrimPrefix(key, folder.path), "/")[0]
			subFolderNames.Store(subFolderName, true)
		}
		return true
	})
	subFolderNames.Range(func(iName, _ interface{}) bool {
		name := iName.(string)
		subFolders = append(subFolders, NewFolder(path.Join(folder.path, name)+"/", folder.Storage))
		return true
	})
	return
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectName := range objectRelativePaths {
		folder.Storage.Delete(storage.JoinPath(folder.path, objectName))
	}
	return nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(path.Join(folder.path, subFolderRelativePath)+"/", folder.Storage)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectAbsPath := path.Join(folder.path, objectRelativePath)
	object, exists := folder.Storage.Load(objectAbsPath)
	if !exists {
		return nil, storage.NewObjectNotFoundError(objectAbsPath)
	}
	return io.NopCloser(&object.Data), nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	data, err := io.ReadAll(content)
	objectPath := path.Join(folder.path, name)
	if err != nil {
		return errors.Wrapf(err, "failed to put '%s' in memory storage", objectPath)
	}
	folder.Storage.Store(objectPath, *bytes.NewBuffer(data))
	return nil
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return errors.New("object does not exist")
		} else {
			return err
		}
	}
	file, err := folder.ReadObject(srcPath)
	if err != nil {
		return err
	}
	err = folder.PutObject(dstPath, file)
	if err != nil {
		return err
	}
	return nil
}
