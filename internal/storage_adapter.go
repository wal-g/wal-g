package internal

import (
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/storages/azure"
	"github.com/wal-g/storages/fs"
	"github.com/wal-g/storages/gcs"
	"github.com/wal-g/storages/s3"
	"github.com/wal-g/storages/sh"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/storages/swift"
)

type StorageAdapter struct {
	prefixName         string // actually this is an env key suffix, but all names contains '_PREFIX' word at the end, see StorageAdapters
	settingNames       []string
	configureFolder    func(string, map[string]string) (storage.Folder, error)
	prefixPreprocessor func(string) string
}

func (adapter *StorageAdapter) loadSettings(config *viper.Viper) map[string]string {
	settings := make(map[string]string)

	for _, settingName := range adapter.settingNames {
		settingValue := config.GetString(settingName)
		if len(settingValue) > 0 {
			settings[settingName] = settingValue
			/* prefer config values */
			continue
		}

		settingValue, ok := getWaleCompatibleSettingFrom(settingName, config)
		if !ok {
			settingValue, ok = GetSetting(settingName)
		}
		if ok {
			settings[settingName] = settingValue
		}
	}
	return settings
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
	{"SSH_PREFIX", sh.SettingsList, sh.ConfigureFolder, nil},
}
