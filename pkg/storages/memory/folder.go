package memory

import (
	"bytes"
	"context"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/contextio"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TODO: Unit tests
type Folder struct {
	path string
	KVS  *KVS
}

func NewFolder(path string, kvs *KVS) *Folder {
	path = strings.TrimPrefix(path, "/")
	return &Folder{path, kvs}
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	_, exists := folder.KVS.Load(path.Join(folder.path, objectRelativePath))
	return exists, nil
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	subFolderNames := sync.Map{}
	folder.KVS.Range(func(key string, value TimeStampedData) bool {
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
		subFolders = append(subFolders, NewFolder(path.Join(folder.path, name)+"/", folder.KVS))
		return true
	})
	return
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectName := range objectRelativePaths {
		folder.KVS.Delete(storage.JoinPath(folder.path, objectName))
	}
	return nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(path.Join(folder.path, subFolderRelativePath)+"/", folder.KVS)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	objectAbsPath := path.Join(folder.path, objectRelativePath)
	object, exists := folder.KVS.Load(objectAbsPath)
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
	folder.KVS.Store(objectPath, *bytes.NewBuffer(data))
	return nil
}

func (folder *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	ctxReader := contextio.NewReader(ctx, content)
	return folder.PutObject(name, ctxReader)
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return err
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

func (folder *Folder) Validate() error {
	return nil
}
