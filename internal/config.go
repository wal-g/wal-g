package internal

import (
	"github.com/spf13/cobra"
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

	MongoDBUriSetting             = "MONGODB_URI"
	MongoDBLastWriteUpdateSeconds = "MONGODB_LAST_WRITE_UPDATE_SECONDS"
	OplogArchiveAfterSize         = "OPLOG_ARCHIVE_AFTER_SIZE"
	OplogArchiveTimeoutSetting    = "OPLOG_ARCHIVE_TIMEOUT"

	MysqlDatasourceNameSetting = "WALG_MYSQL_DATASOURCE_NAME"
	MysqlSslCaSetting          = "WALG_MYSQL_SSL_CA"
	MysqlBinlogReplayCmd       = "WALG_MYSQL_BINLOG_REPLAY_COMMAND"
	MysqlBinlogDstSetting      = "WALG_MYSQL_BINLOG_DST"
	MysqlBackupPrepareCmd      = "WALG_MYSQL_BACKUP_PREPARE_COMMAND"
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

		// MySQL
		MysqlDatasourceNameSetting: true,
		MysqlSslCaSetting:          true,
		MysqlBinlogReplayCmd:       true,
		MysqlBinlogDstSetting:      true,
		MysqlBackupPrepareCmd:      true,
	}

	RequiredSettings = make(map[string]bool)
)

func isAllowedSetting(setting string, AllowedSettings map[string]bool) (exists bool) {
	_, exists = AllowedSettings[setting]
	return
}

func GetSetting(key string) (value string, ok bool) {
	if viper.IsSet(key) {
		return viper.GetString(key), true
	}
	return "", false
}

func getWaleCompatibleSetting(key string) (value string, exists bool) {
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

func AddConfigFlags(Cmd *cobra.Command) {
	for k := range AllowedSettings {
		flagName := toFlagName(k)
		isRequired, exist := RequiredSettings[k]
		flagUsage := ""
		if exist && isRequired {
			flagUsage = "Required, can be set though this flag or " + k + " variable"
		}

		Cmd.PersistentFlags().String(flagName, "", flagUsage)
		_ = viper.BindPFlag(k, Cmd.PersistentFlags().Lookup(flagName))
	}
}

// initConfig reads in config file and ENV variables if set.
func InitConfig() {
	if CfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(CfgFile)
	} else {
		// Find home directory.
		usr, err := user.Current()
		tracelog.ErrorLogger.FatalOnError(err)

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
		tracelog.DebugLogger.Println("Using config file:", viper.ConfigFileUsed())
	}

	checkAndWarnNotAllowedSettings()

	// Set compiled config to ENV.
	// Applicable for Swift/Postgres/etc libs that waiting config paramenters only from ENV.
	for k, v := range viper.AllSettings() {
		val, ok := v.(string)
		if ok {
			bindToEnv(k, val)
		}
	}
}

func checkAndWarnNotAllowedSettings() {
	// Сheck allowed settings
	foundNotAllowed := false
	for k := range viper.AllSettings() {
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

func bindToEnv(k string, val string) {
	if err := os.Setenv(strings.ToUpper(k), val); err != nil {
		tracelog.ErrorLogger.Println("failed to bind config to env variable", err.Error())
		os.Exit(1)
	}
}

func AssertRequiredSettingsSet() {
	if !isAnyStorageSet() {
		tracelog.ErrorLogger.Println("Failed to find any configured storage")
		os.Exit(1)
	}

	for setting, required := range RequiredSettings {
		isSet := viper.IsSet(setting)

		if !isSet && required {
			message := "Required variable " + setting + " is not set. You can set is using --" + toFlagName(setting) +
				" flag or variable " + setting
			tracelog.ErrorLogger.Println(message)
			os.Exit(1)
		}
	}
}

func isAnyStorageSet() bool {
	for _, adapter := range StorageAdapters {
		_, exists := getWaleCompatibleSetting(adapter.prefixName)
		if exists {
			return true
		}
	}

	return false
}

func toFlagName(s string) string {
	return strings.ReplaceAll(strings.ToLower(s), "_", "-")
}
