package config

import (
	"os"
	"strings"
	// "github.com/wal-g/wal-g/internal/storage"
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
	}
)

func LookupConfigValue(key string) (value string, ok bool) {
	if WalgConfig != nil {
		if val, ok := (*WalgConfig)[key]; ok {
			return val, true
		}
	}
	return os.LookupEnv(key)
}

func ToWalgSettingName(waleSettingName string) string {
	return "WALG" + strings.TrimPrefix(waleSettingName, "WALE")
}

func GetSettingValue(key string) string {
	if strings.HasPrefix(key, "WALE") {
		walgKey := ToWalgSettingName(key)
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

func CheckAllowed(field string) bool {
	_, ok := allowedConfigKeys[field]
	return ok
}
