package internal

import (
	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/azure"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/gcs"
	"github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/sh"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/swift"
)

type StorageAdapter struct {
	storageType  string
	settingNames []string
	configure    ConfigureStorageFunc
}

type ConfigureStorageFunc func(
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error)

func (adapter *StorageAdapter) PrefixSettingKey() string {
	return adapter.storageType + "_PREFIX"
}

func (adapter *StorageAdapter) loadSettings(config *viper.Viper) map[string]string {
	settings := make(map[string]string)

	for _, settingName := range adapter.settingNames {
		settingValue := config.GetString(settingName)
		if config.IsSet(settingName) {
			settings[settingName] = settingValue
			/* prefer config values */
			continue
		}

		settingValue, ok := conf.GetWaleCompatibleSettingFrom(settingName, config)
		if !ok {
			settingValue, ok = conf.GetSetting(settingName)
		}
		if ok {
			settings[settingName] = settingValue
		}
	}
	return settings
}

var StorageAdapters = []StorageAdapter{
	{"S3", s3.SettingList, s3.ConfigureStorage},
	{"FILE", nil, fs.ConfigureStorage},
	{"GS", gcs.SettingList, gcs.ConfigureStorage},
	{"AZ", azure.SettingList, azure.ConfigureStorage},
	{"SWIFT", swift.SettingList, swift.ConfigureStorage},
	{"SSH", sh.SettingList, sh.ConfigureStorage},
}
