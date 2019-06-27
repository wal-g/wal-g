package internal

import (
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/tracelog"
	"os/user"
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
	CseKmsIDSetting              = "WALG_CSE_KMS_ID"
	CseKmsRegionSetting          = "WALG_CSE_KMS_REGION"
	GpgKeyIDSetting              = "GPG_KEY_ID"
	PgpKeySetting                = "WALG_PGP_KEY"
	PgpKeyPathSetting            = "WALG_PGP_KEY_PATH"
	PgpKeyPassphraseSetting      = "WALG_PGP_KEY_PASSPHRASE"
	PgDataSetting                = "PGDATA" // TODO : do something with it
	UserSetting                  = "USER"   // TODO : do something with it
	PgPortSetting                = "PGPORT" // TODO : do something with it
	PgUserSetting                = "PGUSER" // TODO : do something with it
	PgHostSetting                = "PGHOST" // TODO : do something with it
	TotalBgUploadedLimit         = "TOTAL_BG_UPLOADED_LIMIT"
)

var (
	CfgFile             string
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
		TotalBgUploadedLimit:         "32",
	}
)

func GetSetting(key string) (value string, ok bool) {
	if viper.IsSet(key) {
		return viper.GetString(key), true
	}
	return "", false
}

func GetWaleCompatibleSetting(key string) (value string, exists bool) {
	settingKeys := []string{
		"WALG_" + key,
		"WALE_" + key,
	}
	// At first we try to check whether it is configured at all
	for _, settingKey := range settingKeys {
		if viper.IsSet(settingKey) {
			return viper.GetString(settingKey), true
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

func Configure() {
	err := ConfigureLogging()
	if err != nil {
		tracelog.ErrorLogger.Println("Failed to configure logging.")
		tracelog.ErrorLogger.FatalError(err)
	}

	ConfigureLimiters()
}

// initConfig reads in config file and ENV variables if set.
func InitConfig() {
	if CfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(CfgFile)
	} else {
		// Find home directory.
		usr, err := user.Current()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}

		// Search config in home directory with name ".wal-g" (without extension).
		viper.AddConfigPath(usr.HomeDir)
		viper.SetConfigName(".walg")
	}

	viper.AutomaticEnv() // read in environment variables that match

	for setting, value := range defaultConfigValues {
		viper.SetDefault(setting, value)
	}

	// If a config file is found, read it in.
	err := viper.ReadInConfig()
	if err == nil {
		tracelog.InfoLogger.Println("Using config file:", viper.ConfigFileUsed())
	}
}
