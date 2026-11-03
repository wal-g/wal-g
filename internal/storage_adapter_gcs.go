//go:build storage_gcs || !(storage_azure || storage_fs || storage_gcs || storage_oss || storage_s3 || storage_sh || storage_swift)

package internal

import "github.com/wal-g/wal-g/pkg/storages/gcs"

func init() {
	registerStorageAdapter(StorageAdapter{"GS", gcs.SettingList, gcs.ConfigureStorage})
}
