package gcs

import (
	"context"
	"fmt"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	client     *gcs.Client
	hash       string
}

type Config struct {
	Secrets         *Secrets `json:"-"`
	RootPath        string
	Bucket          string
	NormalizePrefix bool
	ContextTimeout  time.Duration
	Uploader        *UploaderConfig
}

type Secrets struct {
	EncryptionKey []byte
}

type UploaderConfig struct {
	MaxChunkSize int64
	MaxRetries   int
}

// TODO: unit tests
func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	ctx := context.Background()
	client, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}
	bucket := client.Bucket(config.Bucket)

	var folder storage.Folder = NewFolder(bucket, config.RootPath, config.Secrets.EncryptionKey, config)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	hash, err := storage.ComputeConfigHash("gcs", config)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{folder, client, hash}, nil
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) ConfigHash() string {
	return s.hash
}

func (s *Storage) Close() error {
	err := s.client.Close()
	if err != nil {
		return fmt.Errorf("close GCS client: %w", err)
	}
	return nil
}
