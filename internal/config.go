package internal

import (
	"fmt"
	"os"
	"os/user"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/webserver"
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
	UseReverseUnpackSetting      = "WALG_USE_REVERSE_UNPACK"
	SkipRedundantTarsSetting     = "WALG_SKIP_REDUNDANT_TARS"
	VerifyPageChecksumsSetting   = "WALG_VERIFY_PAGE_CHECKSUMS"
	StoreAllCorruptBlocksSetting = "WALG_STORE_ALL_CORRUPT_BLOCKS"
	UseRatingComposerSetting     = "WALG_USE_RATING_COMPOSER"
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
	PgSlotName                   = "WALG_SLOTNAME"
	TotalBgUploadedLimit         = "TOTAL_BG_UPLOADED_LIMIT"
	NameStreamCreateCmd          = "WALG_STREAM_CREATE_COMMAND"
	NameStreamRestoreCmd         = "WALG_STREAM_RESTORE_COMMAND"

	MongoDBUriSetting               = "MONGODB_URI"
	MongoDBLastWriteUpdateInterval  = "MONGODB_LAST_WRITE_UPDATE_INTERVAL"
	OplogArchiveAfterSize           = "OPLOG_ARCHIVE_AFTER_SIZE"
	OplogArchiveTimeoutInterval     = "OPLOG_ARCHIVE_TIMEOUT_INTERVAL"
	OplogPITRDiscoveryInterval      = "OPLOG_PITR_DISCOVERY_INTERVAL"
	OplogPushStatsEnabled           = "OPLOG_PUSH_STATS_ENABLED"
	OplogPushStatsLoggingInterval   = "OPLOG_PUSH_STATS_LOGGING_INTERVAL"
	OplogPushStatsUpdateInterval    = "OPLOG_PUSH_STATS_UPDATE_INTERVAL"
	OplogPushStatsExposeHttp        = "OPLOG_PUSH_STATS_EXPOSE_HTTP"
	OplogPushWaitForBecomePrimary   = "OPLOG_PUSH_WAIT_FOR_BECOME_PRIMARY"
	OplogPushPrimaryCheckInterval   = "OPLOG_PUSH_PRIMARY_CHECK_INTERVAL"
	OplogReplayOplogAlwaysUpsert    = "OPLOG_REPLAY_OPLOG_ALWAYS_UPSERT"
	OplogReplayOplogApplicationMode = "OPLOG_REPLAY_OPLOG_APPLICATION_MODE"
	OplogReplayIgnoreErrorCodes     = "OPLOG_REPLAY_IGNORE_ERROR_CODES"

	MysqlDatasourceNameSetting = "WALG_MYSQL_DATASOURCE_NAME"
	MysqlSslCaSetting          = "WALG_MYSQL_SSL_CA"
	MysqlBinlogReplayCmd       = "WALG_MYSQL_BINLOG_REPLAY_COMMAND"
	MysqlBinlogDstSetting      = "WALG_MYSQL_BINLOG_DST"
	MysqlBackupPrepareCmd      = "WALG_MYSQL_BACKUP_PREPARE_COMMAND"
	MysqlTakeBinlogsFromMaster = "WALG_MYSQL_TAKE_BINLOGS_FROM_MASTER"

	GoMaxProcs = "GOMAXPROCS"

	HttpListen       = "HTTP_LISTEN"
	HttpExposePprof  = "HTTP_EXPOSE_PPROF"
	HttpExposeExpVar = "HTTP_EXPOSE_EXPVAR"

	SQLServerBlobHostname     = "SQLSERVER_BLOB_HOSTNAME"
	SQLServerBlobCertFile     = "SQLSERVER_BLOB_CERT_FILE"
	SQLServerBlobKeyFile      = "SQLSERVER_BLOB_KEY_FILE"
	SQLServerConnectionString = "SQLSERVER_CONNECTION_STRING"

	EndpointSourceSetting = "S3_ENDPOINT_SOURCE"
	EndpointPortSetting   = "S3_ENDPOINT_PORT"

	AwsAccessKeyId     = "AWS_ACCESS_KEY_ID"
	AwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
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
		UseReverseUnpackSetting:      "false",
		SkipRedundantTarsSetting:     "false",
		VerifyPageChecksumsSetting:   "false",
		StoreAllCorruptBlocksSetting: "false",
		UseRatingComposerSetting:     "false",

		OplogArchiveTimeoutInterval:    "60s",
		OplogArchiveAfterSize:          "16777216", // 32 << (10 * 2)
		MongoDBLastWriteUpdateInterval: "3s",
		OplogPushStatsLoggingInterval:  "30s",
		OplogPushStatsUpdateInterval:   "30s",
		OplogPushWaitForBecomePrimary:  "false",
		OplogPushPrimaryCheckInterval:  "30s",
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
		UseReverseUnpackSetting:      true,
		SkipRedundantTarsSetting:     true,
		VerifyPageChecksumsSetting:   true,
		StoreAllCorruptBlocksSetting: true,
		UseRatingComposerSetting:     true,

		// Postgres
		PgPortSetting:     true,
		PgUserSetting:     true,
		PgHostSetting:     true,
		PgDataSetting:     true,
		PgPasswordSetting: true,
		PgDatabaseSetting: true,
		PgSslModeSetting:  true,
		PgSlotName:        true,
		"PGPASSFILE":      true,

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
		AwsAccessKeyId:                true,
		AwsSecretAccessKey:            true,
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
		"WALG_S3_MAX_PART_SIZE":       true,
		"S3_ENDPOINT_SOURCE":          true,
		"S3_ENDPOINT_PORT":            true,

		// Azure
		"WALG_AZ_PREFIX":          true,
		"AZURE_STORAGE_ACCOUNT":   true,
		"AZURE_STORAGE_KEY":       true,
		"AZURE_STORAGE_SAS_TOKEN": true,
		"WALG_AZURE_BUFFER_SIZE":  true,
		"WALG_AZURE_MAX_BUFFERS":  true,

		// GS
		"WALG_GS_PREFIX":                 true,
		"GOOGLE_APPLICATION_CREDENTIALS": true,

		// SH
		"WALG_SSH_PREFIX":      true,
		"SSH_PORT":             true,
		"SSH_PASSWORD":         true,
		"SSH_USERNAME":         true,
		"SSH_PRIVATE_KEY_PATH": true,

		//File
		"WALG_FILE_PREFIX": true,

		// MongoDB
		MongoDBUriSetting:              true,
		MongoDBLastWriteUpdateInterval: true,
		OplogArchiveTimeoutInterval:    true,
		OplogArchiveAfterSize:          true,
		OplogPushStatsEnabled:          true,
		OplogPushStatsLoggingInterval:  true,
		OplogPushStatsUpdateInterval:   true,
		OplogPushStatsExposeHttp:       true,
		OplogPushWaitForBecomePrimary:  true,
		OplogPushPrimaryCheckInterval:  true,
		OplogPITRDiscoveryInterval:     true,

		// MySQL
		MysqlDatasourceNameSetting: true,
		MysqlSslCaSetting:          true,
		MysqlBinlogReplayCmd:       true,
		MysqlBinlogDstSetting:      true,
		MysqlBackupPrepareCmd:      true,
		MysqlTakeBinlogsFromMaster: true,

		// GOLANG
		GoMaxProcs: true,

		// Web server
		HttpListen:       true,
		HttpExposePprof:  true,
		HttpExposeExpVar: true,

		// SQLServer
		SQLServerBlobHostname:     true,
		SQLServerBlobCertFile:     true,
		SQLServerBlobKeyFile:      true,
		SQLServerConnectionString: true,
	}

	RequiredSettings       = make(map[string]bool)
	HttpSettingExposeFuncs = map[string]func(webserver.WebServer){
		HttpExposePprof:          webserver.EnablePprofEndpoints,
		HttpExposeExpVar:         webserver.EnableExpVarEndpoints,
		OplogPushStatsExposeHttp: nil,
	}
)

func isAllowedSetting(setting string, AllowedSettings map[string]bool) (exists bool) {
	_, exists = AllowedSettings[setting]
	return
}

// GetSetting extract setting by key if key is set, return empty string otherwise
func GetSetting(key string) (value string, ok bool) {
	if viper.IsSet(key) {
		return viper.GetString(key), true
	}
	return "", false
}

func getWaleCompatibleSetting(key string) (value string, exists bool) {
	return getWaleCompatibleSettingFrom(key, viper.GetViper())
}

func getWaleCompatibleSettingFrom(key string, config *viper.Viper) (value string, exists bool) {
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

// ConfigureAndRunDefaultWebServer configures and runs web server
func ConfigureAndRunDefaultWebServer() error {
	var ws webserver.WebServer
	httpListenAddr, httpListen := GetSetting(HttpListen)
	if httpListen {
		ws = webserver.NewSimpleWebServer(httpListenAddr)
		if err := ws.Serve(); err != nil {
			return err
		}
		if err := webserver.SetDefaultWebServer(ws); err != nil {
			return err
		}
	}
	for setting, registerFunc := range HttpSettingExposeFuncs {
		enabled, err := GetBoolSettingDefault(setting, false)
		if err != nil {
			return err
		}
		if !enabled {
			continue
		}
		if !httpListen {
			return fmt.Errorf("%s failed: %s is not set", setting, HttpListen)
		}
		if registerFunc == nil {
			continue
		}
		registerFunc(ws)
	}
	return nil
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

// InitConfig reads config file and ENV variables if set.
func InitConfig() {
	var globalViper = viper.GetViper()
	globalViper.AutomaticEnv() // read in environment variables that match
	SetDefaultValues(globalViper)
	ReadConfigFromFile(globalViper, CfgFile)
	CheckAllowedSettings(globalViper)

	// Set compiled config to ENV.
	// Applicable for Swift/Postgres/etc libs that waiting config paramenters only from ENV.
	for k, v := range viper.AllSettings() {
		val, ok := v.(string)
		if ok {
			err := bindToEnv(k, val)
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}
}

// ReadConfigFromFile read config to the viper instance
func ReadConfigFromFile(config *viper.Viper, configFile string) {
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
	} else {
		if config.ConfigFileUsed() != "" {
			// Config file is found, but parsing failed
			tracelog.WarningLogger.Printf("Failed to parse config file %s. %s.", config.ConfigFileUsed(), err)
		}
	}
}

// SetDefaultValues set default settings to the viper instance
func SetDefaultValues(config *viper.Viper) {
	for setting, value := range defaultConfigValues {
		config.SetDefault(setting, value)
	}

	setGoMaxProcs()
}

func setGoMaxProcs() {
	gomaxprocs := viper.GetInt(GoMaxProcs)
	if gomaxprocs > 0 {
		runtime.GOMAXPROCS(gomaxprocs)
	}
}

// CheckAllowedSettings warnings if a viper instance's setting not allowed
func CheckAllowedSettings(config *viper.Viper) {
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

func bindToEnv(k string, val string) error {
	if err := os.Setenv(strings.ToUpper(k), val); err != nil {
		return errors.Wrap(err, "Failed to bind config to env variable")
	}
	return nil
}

func AssertRequiredSettingsSet() error {
	if !isAnyStorageSet() {
		return errors.New("Failed to find any configured storage")
	}

	for setting, required := range RequiredSettings {
		isSet := viper.IsSet(setting)

		if !isSet && required {
			message := "Required variable " + setting + " is not set. You can set is using --" + toFlagName(setting) +
				" flag or variable " + setting
			return errors.New(message)
		}
	}

	return nil
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

func FolderFromConfig(configFile string) (storage.Folder, error) {
	var config = viper.New()
	SetDefaultValues(config)
	ReadConfigFromFile(config, configFile)
	CheckAllowedSettings(config)

	var folder, err = ConfigureFolderForSpecificConfig(config)

	if err != nil {
		tracelog.ErrorLogger.Println("Failed configure folder according to config " + configFile)
		tracelog.ErrorLogger.FatalError(err)
	}
	return folder, err
}
