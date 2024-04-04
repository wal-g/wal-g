package memory

import (
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	hash       string
}

func NewStorage(rootPath string, kvs *KVS) *Storage {
	return &Storage{
		rootFolder: NewFolder(rootPath, kvs),
		hash:       "mem:" + rootPath,
	}
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (storage *Storage) Move(oldKey string, newKey string) (success bool) {
	valueInterface, ok := storage.underlying.Load(oldKey)
	if !ok {
		return false
	}

	storage.underlying.Store(newKey, valueInterface)
	storage.underlying.CompareAndDelete(oldKey, valueInterface)

	return true
}

func (storage *Storage) Delete(key string) {
	storage.underlying.Delete(key)
  return nil
}

func (s *Storage) Close() error {
	// Nothing to close
	return nil
}

func (s *Storage) ConfigHash() string {
	return s.hash
}
