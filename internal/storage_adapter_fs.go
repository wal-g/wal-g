package internal

import "github.com/wal-g/wal-g/pkg/storages/fs"

func init() {
	registerStorageAdapter(StorageAdapter{"FILE", nil, fs.ConfigureStorage})
}
