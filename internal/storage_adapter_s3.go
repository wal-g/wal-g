//go:build storage_s3 || !(storage_azure || storage_fs || storage_gcs || storage_oss || storage_s3 || storage_sh || storage_swift)

package internal

import "github.com/wal-g/wal-g/pkg/storages/s3"

func init() {
	registerStorageAdapter(StorageAdapter{
		storageType:       "S3",
		settingNames:      s3.SettingList,
		configure:         s3.ConfigureStorage,
		configureFailover: s3.ConfigureStorageWithFailover,
	})
}
