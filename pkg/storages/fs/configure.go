package fs

import (
	"fmt"
	"strings"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const waleFileURL = "file://localhost"

// TODO: Unit tests
func ConfigureStorage(prefix string, _ map[string]string, rootWraps ...storage.WrapRootFolder) (storage.HashableStorage, error) {
	prefix = strings.TrimPrefix(prefix, waleFileURL) // WAL-E backward compatibility

	config := &Config{
		RootPath: prefix,
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create FS storage: %w", err)
	}
	return st, nil
}
