package rclone

import (
	"fmt"
	"os/exec"
	"path"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type Config struct {
	RemoteName        string
	Bucket            string
	RootPath          string
	ConfigPath        string
	RcloneBinary      string
	ExtraArgs         string
	Transfers         int
	BufferSize        int
	Timeout           int
	Retries           int
	LowLevelRetries   int
	S3ChunkSize       int
	UploadConcurrency int
}

type Storage struct {
	config     *Config
	rootFolder storage.Folder
	configHash string
}

func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	rcloneRemotePath := fmt.Sprintf("%s:%s", config.RemoteName, config.Bucket)
	if config.RootPath != "" {
		rcloneRemotePath = path.Join(rcloneRemotePath, config.RootPath)
	}

	var folder storage.Folder = NewFolder(rcloneRemotePath, "", config)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	configHash, err := storage.ComputeConfigHash("RCLONE", config)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{
		config:     config,
		rootFolder: folder,
		configHash: configHash,
	}, nil
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) Close() error {
	return nil
}

func (s *Storage) ConfigHash() string {
	return s.configHash
}

func validateConfig(config *Config) error {
	if config.RemoteName == "" {
		return fmt.Errorf("remote name is required")
	}

	if config.Bucket == "" {
		return fmt.Errorf("bucket/container name is required")
	}

	if _, err := exec.LookPath(config.RcloneBinary); err != nil {
		return fmt.Errorf("rclone binary not found at %q: %w", config.RcloneBinary, err)
	}

	return nil
}
