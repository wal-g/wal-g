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

func (s *Storage) Close() error {
	// Nothing to close
	return nil
}

func (s *Storage) ConfigHash() string {
	return s.hash
}
