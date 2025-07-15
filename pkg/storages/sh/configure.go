package sh

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TODO: Merge the settings and their default values with ones defined in internal/config.go

const (
	portSetting           = "SSH_PORT"
	passwordSetting       = "SSH_PASSWORD"
	usernameSetting       = "SSH_USERNAME"
	privateKeyPathSetting = "SSH_PRIVATE_KEY_PATH"
)

var SettingList = []string{
	portSetting,
	passwordSetting,
	usernameSetting,
	privateKeyPathSetting,
}

const defaultPort = "22"

// TODO: Unit tests
func ConfigureStorage(
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	host, folderPath, err := storage.ParsePrefixAsURL(prefix)
	if err != nil {
		return nil, fmt.Errorf("parse SSH storage prefix %q: %w", prefix, err)
	}

	port := defaultPort
	if p, ok := settings[portSetting]; ok {
		port = p
	}

	config := &Config{
		Secrets: &Secrets{
			Password: settings[passwordSetting],
		},
		Host:           host,
		Port:           port,
		RootPath:       folderPath,
		User:           settings[usernameSetting],
		PrivateKeyPath: settings[privateKeyPathSetting],
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create SSH storage: %w", err)
	}
	return st, nil
}
