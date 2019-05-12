package internal

import (
	"os"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/tracelog"
)

var (
	WalgConfig        *map[string]string
	allowedConfigKeys = map[string]*string{
		"WALG_DOWNLOAD_CONCURRENCY":    nil,
		"WALG_UPLOAD_CONCURRENCY":      nil,
		"WALG_UPLOAD_DISK_CONCURRENCY": nil,
		"WALG_SENTINEL_USER_DATA":      nil,
		"WALG_PREVENT_WAL_OVERWRITE":   nil,
		"WALG_GPG_KEY_ID":              nil,
		"WALE_GPG_KEY_ID":              nil,
		"WALG_PGP_KEY":                 nil,
		"WALG_PGP_KEY_PATH":            nil,
		"WALG_PGP_KEY_PASSPHRASE":      nil,
		"WALG_DELTA_MAX_STEPS":         nil,
		"WALG_DELTA_ORIGIN":            nil,
		"WALG_COMPRESSION_METHOD":      nil,
		"WALG_DISK_RATE_LIMIT":         nil,
		"WALG_NETWORK_RATE_LIMIT":      nil,
		"WALG_USE_WAL_DELTA":           nil,
		"WALG_LOG_LEVEL":               nil,
		"WALG_S3_CA_CERT_FILE":         nil,
		"USER":                         nil,
		"PGPORT":                       nil,
		"PGUSER":                       nil,
		"PGHOST":                       nil,
		"WALG_TAR_SIZE_THRESHOLD":      nil,
	}
)

func init() {
	for _, adapter := range StorageAdapters {
		allowedConfigKeys[adapter.prefixName] = nil
		allowedConfigKeys[toWalgSettingName(adapter.prefixName)] = nil
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

func LookupConfigValue(key string) (value string, ok bool) {
	if WalgConfig != nil {
		if val, ok := (*WalgConfig)[key]; ok {
			return val, true
		}
	}
	return os.LookupEnv(key)
}

func toWalgSettingName(waleSettingName string) string {
	return "WALG" + strings.TrimPrefix(waleSettingName, "WALE")
}

func GetSettingValue(key string) string {
	if strings.HasPrefix(key, "WALE") {
		walgKey := toWalgSettingName(key)
		if val, ok := LookupConfigValue(walgKey); ok && len(val) > 0 {
			return val
		}
	}

	value, ok := LookupConfigValue(key)
	if ok {
		return value
	}
	return ""
}

func UpdateAllowedConfig(fields []string) {
	for _, field := range fields {
		allowedConfigKeys[field] = nil
	}
}
