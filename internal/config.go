package internal

import (
	"os"
	"os/user"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
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
	LibsodiumKeySetting          = "WALG_LIBSODIUM_KEY"
	LibsodiumKeyPathSetting      = "WALG_LIBSODIUM_KEY_PATH"
	GpgKeyIDSetting              = "GPG_KEY_ID"
	PgpKeySetting                = "WALG_PGP_KEY"
	PgpKeyPathSetting            = "WALG_PGP_KEY_PATH"
	PgpKeyPassphraseSetting      = "WALG_PGP_KEY_PASSPHRASE"
	PgDataSetting                = "PGDATA"
	UserSetting                  = "USER" // TODO : do something with it
	PgPortSetting                = "PGPORT"
	PgUserSetting                = "PGUSER"
	PgHostSetting                = "PGHOST"
	PgPasswordSetting            = "PGPASSWORD"
	PgDatabaseSetting            = "PGDATABASE"
	PgSslModeSetting             = "PGSSLMODE"
	TotalBgUploadedLimit         = "TOTAL_BG_UPLOADED_LIMIT"
	NameStreamCreateCmd          = "WALG_STREAM_CREATE_COMMAND"
	NameStreamRestoreCmd         = "WALG_STREAM_RESTORE_COMMAND"
	NameLogApplyCmdPath          = "WALG_LOG_APPLY_COMMAND"

	MongoDBUriSetting             = "MONGODB_URI"
	MongoDBLastWriteUpdateSeconds = "MONGODB_LAST_WRITE_UPDATE_SECONDS"
	OplogArchiveAfterSize         = "OPLOG_ARCHIVE_AFTER_SIZE"
	OplogArchiveTimeoutSetting    = "OPLOG_ARCHIVE_TIMEOUT"
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
		UseWalDeltaSetting:           "false",
		TarSizeThresholdSetting:      "1073741823", // (1 << 30) - 1
		TotalBgUploadedLimit:         "32",

		OplogArchiveTimeoutSetting:    "60",
		OplogArchiveAfterSize:         "33554432", // 32 << (10 * 2)
		MongoDBLastWriteUpdateSeconds: "3",
	}

	AllowedSettings = map[string]bool{
		// WAL-G core
		DownloadConcurrencySetting:   true,
		UploadConcurrencySetting:     true,
		UploadDiskConcurrencySetting: true,
		UploadQueueSetting:           true,
		SentinelUserDataSetting:      true,
		PreventWalOverwriteSetting:   true,
		DeltaMaxStepsSetting:         true,
		DeltaOriginSetting:           true,
		CompressionMethodSetting:     true,
		DiskRateLimitSetting:         true,
		NetworkRateLimitSetting:      true,
		UseWalDeltaSetting:           true,
		LogLevelSetting:              true,
		TarSizeThresholdSetting:      true,
		"WALG_" + GpgKeyIDSetting:    true,
		"WALE_" + GpgKeyIDSetting:    true,
		PgpKeySetting:                true,
		PgpKeyPathSetting:            true,
		PgpKeyPassphraseSetting:      true,
		TotalBgUploadedLimit:         true,
		NameStreamCreateCmd:          true,
		NameStreamRestoreCmd:         true,

		// Postgres
		PgPortSetting:     true,
		PgUserSetting:     true,
		PgHostSetting:     true,
                PgDataSetting:     true,
		PgPasswordSetting: true,
		PgDatabaseSetting: true,
		PgSslModeSetting:  true,

		// Swift
		"WALG_SWIFT_PREFIX": true,
		"OS_AUTH_URL":       true,
		"OS_USERNAME":       true,
		"OS_PASSWORD":       true,
		"OS_TENANT_NAME":    true,
		"OS_REGION_NAME":    true,

		// AWS s3
		"WALG_S3_PREFIX":              true,
		"WALE_S3_PREFIX":              true,
		"AWS_ACCESS_KEY_ID":           true,
		"AWS_SECRET_ACCESS_KEY":       true,
		"AWS_SESSION_TOKEN":           true,
		"AWS_DEFAULT_REGION":          true,
		"AWS_DEFAULT_OUTPUT":          true,
		"AWS_PROFILE":                 true,
		"AWS_ROLE_SESSION_NAME":       true,
		"AWS_CA_BUNDLE":               true,
		"AWS_SHARED_CREDENTIALS_FILE": true,
		"AWS_CONFIG_FILE":             true,
		"AWS_REGION":                  true,
		"AWS_ENDPOINT":                true,
		"AWS_S3_FORCE_PATH_STYLE":     true,
		"WALG_S3_CA_CERT_FILE":        true,
		"WALG_S3_STORAGE_CLASS":       true,
		"WALG_S3_SSE":                 true,
		"WALG_S3_SSE_KMS_ID":          true,
		"WALG_CSE_KMS_ID":             true,
		"WALG_CSE_KMS_REGION":         true,

		// Azure
		"WALG_AZ_PREFIX":         true,
		"AZURE_STORAGE_ACCOUNT":  true,
		"AZURE_STORAGE_KEY":      true,
		"WALG_AZURE_BUFFER_SIZE": true,
		"WALG_AZURE_MAX_BUFFERS": true,

		// GS
		"WALG_GS_PREFIX":                 true,
		"GOOGLE_APPLICATION_CREDENTIALS": true,

		//File
		"WALG_FILE_PREFIX": true,

		// MongoDB
		MongoDBUriSetting:             true,
		MongoDBLastWriteUpdateSeconds: true,
		OplogArchiveTimeoutSetting:    true,
		OplogArchiveAfterSize:         true,
	}
)

func isAllowedSetting(setting string, AllowedSettings map[string]bool) (exists bool) {
	_, exists = AllowedSettings[setting]
	return
}

// GetSetting extract setting by key if key is set, return empty string otherwise
func GetSetting(key string) (value string, ok bool) {
	return GetSettingFrom(viper.GetViper(), key)
}

// GetSettingFrom extract setting by key from config if key is set, return empty string otherwise
func GetSettingFrom(config *viper.Viper, key string) (value string, ok bool) {
	if config.IsSet(key) {
		return config.GetString(key), true
	}
	return "", false
}

func getWaleCompatibleSetting(key string, config *viper.Viper) (value string, exists bool) {
	settingKeys := []string{
		"WALG_" + key,
		"WALE_" + key,
	}
	// At first we try to check whether it is configured at all
	for _, settingKey := range settingKeys {
		if config.IsSet(settingKey) {
			return config.GetString(settingKey), true
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

	// Show all ENV vars in DEVEL Logging Mode
	tracelog.DebugLogger.Println("--- COMPILED ENVIRONMENT VARS ---")
	for _, pair := range os.Environ() {
		tracelog.DebugLogger.Println(pair)
	}

	configureLimiters()

	for _, adapter := range StorageAdapters {
		for _, setting := range adapter.settingNames {
			AllowedSettings[setting] = true
		}
		AllowedSettings["WALG_"+adapter.prefixName] = true
	}
}

// InitConfig reads config file and ENV variables if set.
func InitConfig() {
	var globalViper = viper.GetViper()
	globalViper.AutomaticEnv() // read in environment variables that match
	SetDefaultValues(globalViper)
	ReadConfigFromFile(globalViper, CfgFile)
	CheckAllowedSettings(globalViper)

	// Set compiled config to ENV.
	// Applicable for Swift/Postgres/etc libs that waiting config paramenters only from ENV.
	for k, v := range globalViper.AllSettings() {
		val, ok := v.(string)
		if ok {
			if err := os.Setenv(strings.ToUpper(k), val); err != nil {
				tracelog.ErrorLogger.Println("failed to bind config to env variable", err.Error())
				os.Exit(1)
			}
		}
	}
}

// ReadConfigFromFile read config to the viper instance 
func ReadConfigFromFile(config *viper.Viper, configFile string) (){
	if configFile != "" {
		config.SetConfigFile(configFile)
	} else {
		// Find home directory.
		usr, err := user.Current()
		tracelog.ErrorLogger.FatalOnError(err)

		// Search config in home directory with name ".walg" (without extension).
		config.AddConfigPath(usr.HomeDir)
		config.SetConfigName(".walg")
	}

	// If a config file is found, read it in.
	err := config.ReadInConfig()
	if err == nil {
		tracelog.DebugLogger.Println("Using config file:", config.ConfigFileUsed())
	}
}

// SetDefaultValues set default settings to the viper instance
func SetDefaultValues(config *viper.Viper){
	for setting, value := range defaultConfigValues {
		config.SetDefault(setting, value)
	}
} 

// CheckAllowedSettings warnings if a viper instance's setting not allowed
func CheckAllowedSettings(config *viper.Viper){
	foundNotAllowed := false
	for k := range config.AllSettings() {
		k = strings.ToUpper(k)
		if !isAllowedSetting(k, AllowedSettings) {
			tracelog.WarningLogger.Println(k + " is unknown")
			foundNotAllowed = true
		}
	}

	// TODO delete in the future
	// Message for the first time.
	if foundNotAllowed {
		tracelog.WarningLogger.Println("We found that some variables in your config file detected as 'Unknown'. \n  " +
			"If this is not right, please create issue https://github.com/wal-g/wal-g/issues/new")
	}
}