package internal

import (
	"strings"

	"github.com/tinsane/storages/azure"
	"github.com/tinsane/storages/fs"
	"github.com/tinsane/storages/gcs"
	"github.com/tinsane/storages/s3"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/storages/swift"
)

type StorageAdapter struct {
	prefixName         string
	settingNames       []string
	configureFolder    func(string, map[string]string) (storage.Folder, error)
	prefixPreprocessor func(string) string
}

func (adapter *StorageAdapter) loadSettings() (map[string]string, error) {
	settings := make(map[string]string)
	for _, settingName := range adapter.settingNames {
		settingValue, ok := GetWaleCompatibleSetting(settingName)
		if !ok {
			settingValue, ok = GetSetting(settingName)
		}
		if ok {
			settings[settingName] = settingValue
		}
	}
	return settings, nil
}

func preprocessFilePrefix(prefix string) string {
	return strings.TrimPrefix(prefix, WaleFileHost) // WAL-E backward compatibility
}

var StorageAdapters = []StorageAdapter{
	{"S3_PREFIX", s3.SettingList, s3.ConfigureFolder, nil},
	{"FILE_PREFIX", nil, fs.ConfigureFolder, preprocessFilePrefix},
	{"GS_PREFIX", gcs.SettingList, gcs.ConfigureFolder, nil},
	{"AZ_PREFIX", azure.SettingList, azure.ConfigureFolder, nil},
	{"SWIFT_PREFIX", swift.SettingList, swift.ConfigureFolder, nil},
}
