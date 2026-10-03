package internal

import (
	"context"
	"fmt"
	"slices"

	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type StorageAdapter struct {
	storageType  string
	settingNames []string
	configure    ConfigureStorageFunc
}

type ConfigureStorageFunc func(
	ctx context.Context,
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

type knownStorage struct {
	storageType string
	buildTag    string
}

func (s knownStorage) prefixSettingKey() string {
	return s.storageType + "_PREFIX"
}

// knownStorages lists every storage WAL-G supports, in the order they are tried by ConfigureStorageForSpecificConfig.
// Each storage is compiled in by its own storage_adapter_<name>.go file: when no storage_* build tag is set
// all of them are built, otherwise only the ones whose tag is set.
var knownStorages = []knownStorage{
	{"OSS", "storage_oss"},
	{"S3", "storage_s3"},
	{"FILE", "storage_fs"},
	{"GS", "storage_gcs"},
	{"AZ", "storage_azure"},
	{"SWIFT", "storage_swift"},
	{"SSH", "storage_sh"},
}

// StorageAdapters holds the storages compiled into this binary, ordered as in knownStorages.
var StorageAdapters []StorageAdapter

func knownStorageIndex(storageType string) int {
	return slices.IndexFunc(knownStorages, func(s knownStorage) bool { return s.storageType == storageType })
}

func registerStorageAdapter(adapter StorageAdapter) {
	if knownStorageIndex(adapter.storageType) < 0 {
		panic(fmt.Sprintf("storage type %q is not listed in knownStorages", adapter.storageType))
	}
	StorageAdapters = append(StorageAdapters, adapter)
	slices.SortStableFunc(StorageAdapters, func(a, b StorageAdapter) int {
		return knownStorageIndex(a.storageType) - knownStorageIndex(b.storageType)
	})
}

func isStorageCompiled(storageType string) bool {
	return slices.ContainsFunc(StorageAdapters, func(a StorageAdapter) bool { return a.storageType == storageType })
}

// findNotCompiledStorage returns a storage that is configured via its prefix setting,
// but was left out of this binary by the build tags.
func findNotCompiledStorage(getSetting func(key string) (string, bool)) (knownStorage, bool) {
	for _, s := range knownStorages {
		if isStorageCompiled(s.storageType) {
			continue
		}
		if _, ok := getSetting(s.prefixSettingKey()); ok {
			return s, true
		}
	}
	return knownStorage{}, false
}
