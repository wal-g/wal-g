package swift

import (
	"context"
	"fmt"
	"os"

	"github.com/ncw/swift/v2"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	hash       string
}

type Config struct {
	Container string
	RootPath  string

	// EnvVariables that are conventional for OpenStack: username, key, auth-url, tenantName, region, etc.
	EnvVariables map[string]string

	// SecretEnvVariables are like EnvVariables but require to be kept in secret.
	SecretEnvVariables map[string]string `json:"-"`
}

// TODO: Unit tests
func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	connection := new(swift.Connection)
	for envKey, envValue := range config.EnvVariables {
		err := os.Setenv(envKey, envValue)
		if err != nil {
			return nil, fmt.Errorf("set env variable %q: %w", envKey, err)
		}
	}
	for envKey, envValue := range config.SecretEnvVariables {
		err := os.Setenv(envKey, envValue)
		if err != nil {
			return nil, fmt.Errorf("set secret env variable %q: %w", envKey, err)
		}
	}

	ctx := context.Background()
	err := connection.ApplyEnvironment()
	if err != nil {
		return nil, fmt.Errorf("apply env variables: %w", err)
	}
	err = connection.Authenticate(ctx)
	if err != nil {
		return nil, fmt.Errorf("authenticate: %w", err)
	}

	container, _, err := connection.Container(ctx, config.Container)
	if err != nil {
		return nil, fmt.Errorf("get container by name: %w", err)
	}

	var folder storage.Folder = NewFolder(connection, container, config.RootPath)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	hash, err := storage.ComputeConfigHash("swift", config)
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
	// Nothing to close: Swift connections only keep context and don't require to be closed.
	return nil
}
