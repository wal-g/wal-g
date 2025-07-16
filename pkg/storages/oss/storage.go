package oss

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	hash       string
}

type Config struct {
	AccessKeyID      string
	AccessKeySecret  string
	SecurityToken    string
	Region           string
	Bucket           string
	RootPath         string
	RoleARN          string
	RoleSessionName  string
	SkipValidation   bool
	MaxRetries       int
	EnableVersioning string
}

func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	client, err := configureClient(config)
	if err != nil {
		return nil, fmt.Errorf("configure client: %w", err)
	}

	var folder storage.Folder = NewFolder(client, config.Bucket, config.RootPath, config)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	if !config.SkipValidation {
		err = folder.Validate()
		if err != nil {
			return nil, err
		}
	}

	hash, err := storage.ComputeConfigHash("oss", config)
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
	// Nothing to close: the oss session doesn't require to be closed
	return nil
}
