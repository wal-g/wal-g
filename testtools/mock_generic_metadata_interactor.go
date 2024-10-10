package testtools

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type MockGenericMetaFetcher struct {
	MockMeta map[string]internal.GenericMetadata
}

func (m *MockGenericMetaFetcher) Fetch(backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	return m.MockMeta[backupName], nil
}
