package oci

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
	Region            string
	TenancyOCID       string
	SecurityTokenFile string
	PrivateKeyFile    string
	ConfigFile        string
	Profile           string
	CACertFile        string
	Bucket            string
	RootPath          string
	ConnectTimeout    int64
}

// NewStorage creates a new OCI storage instance with the given configuration.
func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	client, err := configureClient(config)
	if err != nil {
		return nil, fmt.Errorf("configure OCI client: %w", err)
	}

	var folder storage.Folder = NewFolder(client, config.Bucket, config.RootPath, config.Region)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	// Compute hash only from storage location identifiers, not credential paths
	hashConfig := struct {
		Region      string
		TenancyOCID string
		Bucket      string
		RootPath    string
	}{
		Region:      config.Region,
		TenancyOCID: config.TenancyOCID,
		Bucket:      config.Bucket,
		RootPath:    config.RootPath,
	}

	hash, err := storage.ComputeConfigHash("oci", hashConfig)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{folder, hash}, nil
}

// RootFolder returns the root folder for this storage.
func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

// ConfigHash returns a hash of the storage configuration.
func (s *Storage) ConfigHash() string {
	return s.hash
}

// Close is a no-op for OCI storage.
// WAL-G spawns short-lived processes so connection cleanup is handled by process exit.
func (s *Storage) Close() error {
	return nil
}
