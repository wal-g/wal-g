package internal

import (
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/tracelog"
	"os"
)

const (
	DownloadConcurrencySetting   = "WALG_DOWNLOAD_CONCURRENCY"
	UploadConcurrencySetting     = "WALG_UPLOAD_CONCURRENCY"
	UploadDiskConcurrencySetting = "WALG_UPLOAD_DISK_CONCURRENCY"
	UploadQueueSetting           = "WALG_UPLOAD_QUEUE"
	SentinelUserDataSetting      = "WALG_SENTINEL_USER_DATA"
	PreventWalOverwriteSetting   = "WALG_PREVENT_WAL_OVERWRITE"
	DeltaMaxStepsSetting         = "WALG_DELTA_MAX_STEPS"
	DeltaOriginSetting           = "WALG_DELTA_ORIGIN"
	CompressionMethodSetting     = "WALG_COMPRESSION_METHOD"
	DiskRateLimitSetting         = "WALG_DISK_RATE_LIMIT"
	NetworkRateLimitSetting      = "WALG_NETWORK_RATE_LIMIT"
	UseWalDeltaSetting           = "WALG_USE_WAL_DELTA"
	LogLevelSetting              = "WALG_LOG_LEVEL"
	TarSizeThresholdSetting      = "WALG_TAR_SIZE_THRESHOLD"
	GpgKeyIDSetting              = "GPG_KEY_ID"
	PgpKeySetting                = "WALG_PGP_KEY"
	PgpKeyPathSetting            = "WALG_PGP_KEY_PATH"
	PgpKeyPassphraseSetting      = "WALG_PGP_KEY_PASSPHRASE"
)

var (
	WalgConfig        *map[string]string
	allowedConfigKeys = map[string]*string{
		DownloadConcurrencySetting:   nil,
		UploadConcurrencySetting:     nil,
		UploadDiskConcurrencySetting: nil,
		UploadQueueSetting:           nil,
		SentinelUserDataSetting:      nil,
		PreventWalOverwriteSetting:   nil,
		"WALG_" + GpgKeyIDSetting:    nil,
		"WALE_" + GpgKeyIDSetting:    nil,
		PgpKeySetting:                nil,
		PgpKeyPathSetting:            nil,
		PgpKeyPassphraseSetting:      nil,
		DeltaMaxStepsSetting:         nil,
		DeltaOriginSetting:           nil,
		CompressionMethodSetting:     nil,
		DiskRateLimitSetting:         nil,
		NetworkRateLimitSetting:      nil,
		UseWalDeltaSetting:           nil,
		LogLevelSetting:              nil,
		TarSizeThresholdSetting:      nil,
		"USER":                       nil, // TODO : do something with it
		"PGPORT":                     nil, // TODO : do something with it
		"PGUSER":                     nil, // TODO : do something with it
		"PGHOST":                     nil, // TODO : do something with it
	}
	defaultConfigValues = map[string]string{
		DownloadConcurrencySetting:   "10",
		UploadConcurrencySetting:     "16",
		UploadDiskConcurrencySetting: "1",
		UploadQueueSetting:           "2",
		PreventWalOverwriteSetting:   "false",
		DeltaMaxStepsSetting:         "0",
		CompressionMethodSetting:     "lz4",
		UseWalDeltaSetting:           "true",
		TarSizeThresholdSetting:      "1073741823", // (1 << 30) - 1
	}
)

func GetSetting(key string) (value string, ok bool) {
	if WalgConfig != nil {
		if val, ok := (*WalgConfig)[key]; ok {
			return val, true
		}
	}
	return os.LookupEnv(key)
}

func GetSettingWithDefault(key string) string {
	value, ok := GetSetting(key)
	if ok {
		return value
	}
	return defaultConfigValues[key]
}

func GetWaleCompatibleSetting(key string) (value string, exists bool) {
	settingKeys := []string{
		"WALG_" + key,
		"WALE_" + key,
	}
	// At first we try to check whether it is configured at all
	for _, settingKey := range settingKeys {
		if val, ok := GetSetting(settingKey); ok && len(val) > 0 {
			return val, true
		}
	}
	// Then we try to get default value
	for _, settingKey := range settingKeys {
		if val, ok := defaultConfigValues[settingKey]; ok && len(val) > 0 {
			return val, true
		}
	}
	return "", false
}

func UpdateAllowedConfig(fields []string) {
	for _, field := range fields {
		allowedConfigKeys[field] = nil
	}
}

func init() {
	for _, adapter := range StorageAdapters {
		allowedConfigKeys["WALG_"+adapter.prefixName] = nil
		allowedConfigKeys["WALE_"+adapter.prefixName] = nil
		for _, settingName := range adapter.settingNames {
			allowedConfigKeys["WALG_"+settingName] = nil
			allowedConfigKeys["WALE_"+settingName] = nil
			allowedConfigKeys[settingName] = nil
		}
	}
	readConfig()
	verifyConfig()
}

func verifyConfig() {
	if WalgConfig == nil {
		return
	}
	for _, extension := range Extensions {
		for key, value := range extension.GetAllowedConfigKeys() {
			allowedConfigKeys[key] = value
		}
	}
	for k := range *WalgConfig {
		if _, ok := allowedConfigKeys[k]; !ok {
			tracelog.ErrorLogger.Panic("Settings " + k + " is unknown")
		}
	}
}

func readConfig() {
	cfg := make(map[string]string)
	WalgConfig = &cfg
	for _, key := range viper.AllKeys() {
		cfg[key] = viper.GetString(key)
	}
}
