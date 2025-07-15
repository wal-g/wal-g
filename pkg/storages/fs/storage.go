package fs

import (
	"fmt"
	"os"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	hash       string
}

type Config struct {
	// RootPath points to the directory on the FS where this storage is located. All objects are created inside this dir.
	RootPath string
}

// TODO: Unit tests
func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	if _, err := os.Stat(config.RootPath); err != nil {
		return nil, fmt.Errorf("FS storage root directory doesn't exist or is inaccessible: %w", err)
	}

	var folder storage.Folder = NewFolder(config.RootPath, "")

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	hash, err := storage.ComputeConfigHash("fs", config)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{folder, hash}, nil
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) ConfigHash() string {
	return s.hash
}

func (s *Storage) Close() error {
	// Nothing to close
	return nil
}
