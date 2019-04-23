package internal

import (
	"github.com/wal-g/wal-g/internal/storages/azure"
	"github.com/wal-g/wal-g/internal/storages/fs"
	"github.com/wal-g/wal-g/internal/storages/gcs"
	"github.com/wal-g/wal-g/internal/storages/s3"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/storages/swift"
	"strconv"
	"strings"
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
		if settingName == "UPLOAD_CONCURRENCY" {
			concurrency, err := getMaxUploadConcurrency(10)
			if err != nil {
				return nil, err
			}
			settings[settingName] = strconv.Itoa(concurrency)
			continue
		}
		settingValue := GetSettingValue("WALE_" + settingName)
		if settingValue == "" {
			settingValue = GetSettingValue(settingName)
		}
		if settingValue != "" {
			settings[settingName] = settingValue
		}
	}
	return settings, nil
}

func preprocessFilePrefix(prefix string) string {
	return strings.TrimPrefix(prefix, WaleFileHost) // WAL-E backward compatibility
}

var StorageAdapters = []StorageAdapter{
	{"WALE_S3_PREFIX", s3.SettingList, s3.ConfigureFolder, nil},
	{"WALE_FILE_PREFIX", nil, fs.ConfigureFolder, preprocessFilePrefix},
	{"WALE_GS_PREFIX", nil, gcs.ConfigureFolder, nil},
	{"WALE_AZ_PREFIX", azure.SettingList, azure.ConfigureFolder, nil},
	{"WALE_SWIFT_PREFIX", nil, swift.ConfigureFolder, nil},
}
