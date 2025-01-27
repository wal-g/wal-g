package config

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/internal/webserver"
)

const (
	PG        = "PG"
	SQLSERVER = "SQLSERVER"
	MYSQL     = "MYSQL"
	REDIS     = "REDIS"
	FDB       = "FDB"
	MONGO     = "MONGO"
	GP        = "GP"
	ETCD      = "ETCD"

	DownloadConcurrencySetting    = "WALG_DOWNLOAD_CONCURRENCY"
	UploadConcurrencySetting      = "WALG_UPLOAD_CONCURRENCY"
	UploadDiskConcurrencySetting  = "WALG_UPLOAD_DISK_CONCURRENCY"
	UploadQueueSetting            = "WALG_UPLOAD_QUEUE"
	DownloadFileRetriesSetting    = "WALG_DOWNLOAD_FILE_RETRIES"
	SentinelUserDataSetting       = "WALG_SENTINEL_USER_DATA"
	PreventWalOverwriteSetting    = "WALG_PREVENT_WAL_OVERWRITE"
	UploadWalMetadata             = "WALG_UPLOAD_WAL_METADATA"
	DeltaMaxStepsSetting          = "WALG_DELTA_MAX_STEPS"
	DeltaOriginSetting            = "WALG_DELTA_ORIGIN"
	CompressionMethodSetting      = "WALG_COMPRESSION_METHOD"
	StoragePrefixSetting          = "WALG_STORAGE_PREFIX"
	DiskRateLimitSetting          = "WALG_DISK_RATE_LIMIT"
	NetworkRateLimitSetting       = "WALG_NETWORK_RATE_LIMIT"
	UseWalDeltaSetting            = "WALG_USE_WAL_DELTA"
	UseReverseUnpackSetting       = "WALG_USE_REVERSE_UNPACK"
	SkipRedundantTarsSetting      = "WALG_SKIP_REDUNDANT_TARS"
	VerifyPageChecksumsSetting    = "WALG_VERIFY_PAGE_CHECKSUMS"
	StoreAllCorruptBlocksSetting  = "WALG_STORE_ALL_CORRUPT_BLOCKS"
	UseRatingComposerSetting      = "WALG_USE_RATING_COMPOSER"
	UseCopyComposerSetting        = "WALG_USE_COPY_COMPOSER"
	UseDatabaseComposerSetting    = "WALG_USE_DATABASE_COMPOSER"
	WithoutFilesMetadataSetting   = "WALG_WITHOUT_FILES_METADATA"
	DeltaFromNameSetting          = "WALG_DELTA_FROM_NAME"
	DeltaFromUserDataSetting      = "WALG_DELTA_FROM_USER_DATA"
	FetchTargetUserDataSetting    = "WALG_FETCH_TARGET_USER_DATA"
	LogLevelSetting               = "WALG_LOG_LEVEL"
	LogDestinationSetting         = "WALG_LOG_DESTINATION"
	TarSizeThresholdSetting       = "WALG_TAR_SIZE_THRESHOLD"
	TarDisableFsyncSetting        = "WALG_TAR_DISABLE_FSYNC"
	CseKmsIDSetting               = "WALG_CSE_KMS_ID"
	CseKmsRegionSetting           = "WALG_CSE_KMS_REGION"
	LibsodiumKeySetting           = "WALG_LIBSODIUM_KEY"
	LibsodiumKeyPathSetting       = "WALG_LIBSODIUM_KEY_PATH"
	LibsodiumKeyTransform         = "WALG_LIBSODIUM_KEY_TRANSFORM"
	GpgKeyIDSetting               = "GPG_KEY_ID"
	PgpKeySetting                 = "WALG_PGP_KEY"
	PgpKeyPathSetting             = "WALG_PGP_KEY_PATH"
	PgpKeyPassphraseSetting       = "WALG_PGP_KEY_PASSPHRASE"
	PgpEnvelopeKeySetting         = "WALG_ENVELOPE_PGP_KEY"
	PgpEnvelopKeyPathSetting      = "WALG_ENVELOPE_PGP_KEY_PATH"
	PgpEnvelopeYcKmsKeyIDSetting  = "WALG_ENVELOPE_PGP_YC_CSE_KMS_KEY_ID"
	PgpEnvelopeYcSaKeyFileSetting = "WALG_ENVELOPE_PGP_YC_SERVICE_ACCOUNT_KEY_FILE"
	PgpEnvelopeYcEndpointSetting  = "WALG_ENVELOPE_PGP_YC_ENDPOINT"
	PgpEnvelopeCacheExpiration    = "WALG_ENVELOPE_CACHE_EXPIRATION"

	PgDataSetting                        = "PGDATA"
	UserSetting                          = "USER" // TODO : do something with it
	PgPortSetting                        = "PGPORT"
	PgUserSetting                        = "PGUSER"
	PgHostSetting                        = "PGHOST"
	PgPasswordSetting                    = "PGPASSWORD"
	PgPassfileSetting                    = "PGPASSFILE"
	PgDatabaseSetting                    = "PGDATABASE"
	PgSslModeSetting                     = "PGSSLMODE"
	PgSslKey                             = "PGSSLKEY"
	PgSslCert                            = "PGSSLCERT"
	PgSslRootCert                        = "PGSSLROOTCERT"
	PgSlotName                           = "WALG_SLOTNAME"
	PgWalSize                            = "WALG_PG_WAL_SIZE"
	TotalBgUploadedLimit                 = "TOTAL_BG_UPLOADED_LIMIT"
	NameStreamCreateCmd                  = "WALG_STREAM_CREATE_COMMAND"
	NameStreamRestoreCmd                 = "WALG_STREAM_RESTORE_COMMAND"
	MaxDelayedSegmentsCount              = "WALG_INTEGRITY_MAX_DELAYED_WALS"
	PrefetchDir                          = "WALG_PREFETCH_DIR"
	PgReadyRename                        = "PG_READY_RENAME"
	SerializerTypeSetting                = "WALG_SERIALIZER_TYPE"
	StreamSplitterPartitions             = "WALG_STREAM_SPLITTER_PARTITIONS"
	StreamSplitterBlockSize              = "WALG_STREAM_SPLITTER_BLOCK_SIZE"
	StreamSplitterMaxFileSize            = "WALG_STREAM_SPLITTER_MAX_FILE_SIZE"
	StatsdAddressSetting                 = "WALG_STATSD_ADDRESS"
	StatsdExtraTagsSetting               = "WALG_STATSD_EXTRA_TAGS"
	PgAliveCheckInterval                 = "WALG_ALIVE_CHECK_INTERVAL"
	PgStopBackupTimeout                  = "WALG_STOP_BACKUP_TIMEOUT"
	FailoverStorages                     = "WALG_FAILOVER_STORAGES"
	FailoverStoragesCheck                = "WALG_FAILOVER_STORAGES_CHECK"
	FailoverStoragesCheckTimeout         = "WALG_FAILOVER_STORAGES_CHECK_TIMEOUT"
	FailoverStorageCacheLifetime         = "WALG_FAILOVER_STORAGES_CACHE_LIFETIME"
	FailoverStorageCacheEMAAliveLimit    = "WALG_FAILOVER_STORAGES_CACHE_EMA_ALIVE_LIMIT"
	FailoverStorageCacheEMADeadLimit     = "WALG_FAILOVER_STORAGES_CACHE_EMA_DEAD_LIMIT"
	FailoverStorageCacheEMAAlphaAliveMax = "WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_ALIVE_MAX"
	FailoverStorageCacheEMAAlphaAliveMin = "WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_ALIVE_MIN"
	FailoverStorageCacheEMAAlphaDeadMax  = "WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_DEAD_MAX"
	FailoverStorageCacheEMAAlphaDeadMin  = "WALG_FAILOVER_STORAGES_CACHE_EMA_ALPHA_DEAD_MIN"
	FailoverStoragesCheckSize            = "WALG_FAILOVER_STORAGES_CHECK_SIZE"
	PgDaemonWALUploadTimeout             = "WALG_DAEMON_WAL_UPLOAD_TIMEOUT"
	PgTargetStorage                      = "WALG_TARGET_STORAGE"

	ProfileSamplingRatio = "PROFILE_SAMPLING_RATIO"
	ProfileMode          = "PROFILE_MODE"
	ProfilePath          = "PROFILE_PATH"

	MongoDBUriSetting                  = "MONGODB_URI"
	MongoDBLastWriteUpdateInterval     = "MONGODB_LAST_WRITE_UPDATE_INTERVAL"
	MongoDBExtendBackupCursor          = "MONGODB_EXTEND_BACKUP_CURSOR"
	MongoDBDeletionProtectionWhitelist = "MONGODB_DELETION_PROTECTION_WHITELIST"
	OplogArchiveAfterSize              = "OPLOG_ARCHIVE_AFTER_SIZE"
	OplogArchiveTimeoutInterval        = "OPLOG_ARCHIVE_TIMEOUT_INTERVAL"
	OplogPITRDiscoveryInterval         = "OPLOG_PITR_DISCOVERY_INTERVAL"
	OplogPushStatsEnabled              = "OPLOG_PUSH_STATS_ENABLED"
	OplogPushStatsLoggingInterval      = "OPLOG_PUSH_STATS_LOGGING_INTERVAL"
	OplogPushStatsUpdateInterval       = "OPLOG_PUSH_STATS_UPDATE_INTERVAL"
	OplogPushStatsExposeHTTP           = "OPLOG_PUSH_STATS_EXPOSE_HTTP"
	OplogPushWaitForBecomePrimary      = "OPLOG_PUSH_WAIT_FOR_BECOME_PRIMARY"
	OplogPushPrimaryCheckInterval      = "OPLOG_PUSH_PRIMARY_CHECK_INTERVAL"
	OplogReplayOplogAlwaysUpsert       = "OPLOG_REPLAY_OPLOG_ALWAYS_UPSERT"
	OplogReplayOplogApplicationMode    = "OPLOG_REPLAY_OPLOG_APPLICATION_MODE"
	OplogReplayIgnoreErrorCodes        = "OPLOG_REPLAY_IGNORE_ERROR_CODES"
	OplogRecoverTimeout                = "OPLOG_RECOVER_TIMEOUT"

	MysqlDatasourceNameSetting     = "WALG_MYSQL_DATASOURCE_NAME"
	MysqlSslCaSetting              = "WALG_MYSQL_SSL_CA"
	MysqlBinlogReplayCmd           = "WALG_MYSQL_BINLOG_REPLAY_COMMAND"
	MysqlBinlogDstSetting          = "WALG_MYSQL_BINLOG_DST"
	MysqlBackupPrepareCmd          = "WALG_MYSQL_BACKUP_PREPARE_COMMAND"
	MysqlCheckGTIDs                = "WALG_MYSQL_CHECK_GTIDS"
	MysqlBinlogServerHost          = "WALG_MYSQL_BINLOG_SERVER_HOST"
	MysqlBinlogServerPort          = "WALG_MYSQL_BINLOG_SERVER_PORT"
	MysqlBinlogServerUser          = "WALG_MYSQL_BINLOG_SERVER_USER"
	MysqlBinlogServerPassword      = "WALG_MYSQL_BINLOG_SERVER_PASSWORD"
	MysqlBinlogServerID            = "WALG_MYSQL_BINLOG_SERVER_ID"
	MysqlBinlogServerReplicaSource = "WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE"
	MysqlBackupDownloadMaxRetry    = "WALG_BACKUP_DOWNLOAD_MAX_RETRY"
	MysqlIncrementalBackupDst      = "WALG_MYSQL_INCREMENTAL_BACKUP_DST"
	MysqlDataDir                   = "WALG_MYSQL_DATA_DIR"
	// Deprecated: unused
	MysqlTakeBinlogsFromMaster = "WALG_MYSQL_TAKE_BINLOGS_FROM_MASTER"

	RedisUsername            = "WALG_REDIS_USERNAME"
	RedisPassword            = "WALG_REDIS_PASSWORD"
	RedisDataPath            = "WALG_REDIS_DATA_PATH"
	RedisAppendonlyFolder    = "WALG_REDIS_APPENDONLY_PATH"
	RedisAppendonlyManifest  = "WALG_REDIS_APPENDONLY_MANIFEST"
	RedisAppendonlyTmpFolder = "WALG_REDIS_APPENDONLY_TEMP_MANIFEST"
	RedisDataThreshold       = "WALG_REDIS_DATA_THRESHOLD"
	RedisDataTimeout         = "WALG_REDIS_DATA_TIMEOUT"
	RedisServerProcessName   = "WALG_REDIS_SERVER_PROCESS_NAME"

	GPLogsDirectory              = "WALG_GP_LOGS_DIR"
	GPSegContentID               = "WALG_GP_SEG_CONTENT_ID"
	GPSegmentsPollInterval       = "WALG_GP_SEG_POLL_INTERVAL"
	GPSegmentsPollRetries        = "WALG_GP_SEG_POLL_RETRIES"
	GPSegmentsUpdInterval        = "WALG_GP_SEG_UPD_INTERVAL"
	GPSegmentStatesDir           = "WALG_GP_SEG_STATES_DIR"
	GPDeleteConcurrency          = "WALG_GP_DELETE_CONCURRENCY"
	GPAoSegSizeThreshold         = "WALG_GP_AOSEG_SIZE_THRESHOLD"
	GPAoDeduplicationAgeLimit    = "WALG_GP_AOSEG_DEDUPLICATION_AGE_LIMIT"
	GPRelativeRecoveryConfPath   = "WALG_GP_RELATIVE_RECOVERY_CONF_PATH"
	GPRelativePostgresqlConfPath = "WALG_GP_RELATIVE_POSTGRESQL_CONF_PATH"

	ETCDMemberDataDirectory = "WALG_ETCD_DATA_DIR"
	ETCDWalDirectory        = "WALG_ETCD_WAL_DIR"

	GoMaxProcs = "GOMAXPROCS"

	HTTPListen       = "HTTP_LISTEN"
	HTTPExposePprof  = "HTTP_EXPOSE_PPROF"
	HTTPExposeExpVar = "HTTP_EXPOSE_EXPVAR"

	SQLServerBlobHostname     = "SQLSERVER_BLOB_HOSTNAME"
	SQLServerBlobCertFile     = "SQLSERVER_BLOB_CERT_FILE"
	SQLServerBlobKeyFile      = "SQLSERVER_BLOB_KEY_FILE"
	SQLServerBlobLockFile     = "SQLSERVER_BLOB_LOCK_FILE"
	SQLServerConnectionString = "SQLSERVER_CONNECTION_STRING"
	SQLServerDBConcurrency    = "SQLSERVER_DB_CONCURRENCY"
	SQLServerReuseProxy       = "SQLSERVER_REUSE_PROXY"

	EndpointSourceSetting = "S3_ENDPOINT_SOURCE"
	EndpointPortSetting   = "S3_ENDPOINT_PORT"

	AwsAccessKeyID     = "AWS_ACCESS_KEY_ID"
	AwsSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	AwsSessionToken    = "AWS_SESSION_TOKEN"

	YcKmsKeyIDSetting  = "YC_CSE_KMS_KEY_ID"
	YcSaKeyFileSetting = "YC_SERVICE_ACCOUNT_KEY_FILE"

	PgBackRestStanza = "PGBACKREST_STANZA"

	AzureStorageAccount   = "AZURE_STORAGE_ACCOUNT"
	AzureStorageAccessKey = "AZURE_STORAGE_ACCESS_KEY"
	AzureStorageSasToken  = "AZURE_STORAGE_SAS_TOKEN"
	AzureEnvironmentName  = "AZURE_ENVIRONMENT_NAME"

	GoogleApplicationCredentials = "GOOGLE_APPLICATION_CREDENTIALS"

	SwiftOsAuthURL    = "OS_AUTH_URL"
	SwiftOsUsername   = "OS_USERNAME"
	SwiftOsPassword   = "OS_PASSWORD"
	SwiftOsTenantName = "OS_TENANT_NAME"
	SwiftOsRegionName = "OS_REGION_NAME"

	SSHPort           = "SSH_PORT"
	SSHPassword       = "SSH_PASSWORD"
	SSHUsername       = "SSH_USERNAME"
	SSHPrivateKeyPath = "SSH_PRIVATE_KEY_PATH"

	SystemdNotifySocket = "NOTIFY_SOCKET"
)

var (
	CfgFile             string
	DefaultConfigValues map[string]string

	CommonDefaultConfigValues = map[string]string{
		DownloadConcurrencySetting:   "10",
		UploadConcurrencySetting:     "16",
		UploadDiskConcurrencySetting: "1",
		UploadQueueSetting:           "2",
		DownloadFileRetriesSetting:   "15",
		PreventWalOverwriteSetting:   "false",
		UploadWalMetadata:            "NOMETADATA",
		DeltaMaxStepsSetting:         "0",
		CompressionMethodSetting:     "lz4",
		UseWalDeltaSetting:           "false",
		TarSizeThresholdSetting:      "1073741823", // (1 << 30) - 1
		TarDisableFsyncSetting:       "false",
		TotalBgUploadedLimit:         "32",
		UseReverseUnpackSetting:      "false",
		SkipRedundantTarsSetting:     "false",
		VerifyPageChecksumsSetting:   "false",
		StoreAllCorruptBlocksSetting: "false",
		UseRatingComposerSetting:     "false",
		UseCopyComposerSetting:       "false",
		UseDatabaseComposerSetting:   "false",
		WithoutFilesMetadataSetting:  "false",
		MaxDelayedSegmentsCount:      "0",
		SerializerTypeSetting:        "json_default",
		LibsodiumKeyTransform:        "none",
		FailoverStoragesCheckTimeout: "30s",
		FailoverStorageCacheLifetime: "15m",
		PgpEnvelopeCacheExpiration:   "0",
		LogLevelSetting:              "NORMAL",
	}

	MongoDefaultSettings = map[string]string{
		OplogPushStatsLoggingInterval:  "30s",
		OplogPushStatsUpdateInterval:   "30s",
		OplogPushWaitForBecomePrimary:  "false",
		OplogPushPrimaryCheckInterval:  "30s",
		OplogArchiveTimeoutInterval:    "60s",
		OplogArchiveAfterSize:          "16777216", // 32 << (10 * 2)
		MongoDBLastWriteUpdateInterval: "3s",
		StreamSplitterBlockSize:        "1048576",
	}

	RedisDefaultSettings = map[string]string{
		RedisDataPath:            "/var/lib/redis",
		RedisAppendonlyFolder:    "appendonlydir",
		RedisAppendonlyManifest:  "appendonly.aof.manifest",
		RedisAppendonlyTmpFolder: "/var/run/wal-g/",
		RedisDataThreshold:       "90",
		RedisDataTimeout:         "1",
		RedisServerProcessName:   "redis-server",
	}

	MysqlDefaultSettings = map[string]string{
		StreamSplitterBlockSize:     "1048576",
		MysqlBackupDownloadMaxRetry: "1",
		MysqlIncrementalBackupDst:   "/tmp",
	}

	SQLServerDefaultSettings = map[string]string{
		SQLServerDBConcurrency: "10",
	}

	PGDefaultSettings = map[string]string{
		PgWalSize:                 "16",
		PgBackRestStanza:          "main",
		PgAliveCheckInterval:      "1m",
		FailoverStoragesCheckSize: "1mb",
		PgDaemonWALUploadTimeout:  "60s",
	}

	GPDefaultSettings = map[string]string{
		GPLogsDirectory:              "/var/log",
		PgWalSize:                    "64",
		GPSegmentsPollInterval:       "5m",
		GPSegmentsUpdInterval:        "10s",
		GPSegmentsPollRetries:        "5",
		GPSegmentStatesDir:           "/tmp",
		GPDeleteConcurrency:          "1",
		GPAoSegSizeThreshold:         "1048576", // (1 << 20)
		GPAoDeduplicationAgeLimit:    "720h",    // 30 days
		GPRelativeRecoveryConfPath:   "recovery.conf",
		GPRelativePostgresqlConfPath: "postgresql.conf",
	}

	AllowedSettings map[string]bool

	CommonAllowedSettings = map[string]bool{
		// WAL-G core
		DownloadConcurrencySetting:    true,
		UploadConcurrencySetting:      true,
		UploadDiskConcurrencySetting:  true,
		UploadQueueSetting:            true,
		DownloadFileRetriesSetting:    true,
		SentinelUserDataSetting:       true,
		PreventWalOverwriteSetting:    true,
		UploadWalMetadata:             true,
		DeltaMaxStepsSetting:          true,
		DeltaOriginSetting:            true,
		CompressionMethodSetting:      true,
		StoragePrefixSetting:          true,
		DiskRateLimitSetting:          true,
		NetworkRateLimitSetting:       true,
		UseWalDeltaSetting:            true,
		LogLevelSetting:               true,
		LogDestinationSetting:         true,
		TarSizeThresholdSetting:       true,
		TarDisableFsyncSetting:        true,
		"WALG_" + GpgKeyIDSetting:     true,
		"WALE_" + GpgKeyIDSetting:     true,
		PgpKeySetting:                 true,
		PgpKeyPathSetting:             true,
		PgpKeyPassphraseSetting:       true,
		PgpEnvelopeKeySetting:         true,
		PgpEnvelopKeyPathSetting:      true,
		PgpEnvelopeCacheExpiration:    true,
		PgpEnvelopeYcKmsKeyIDSetting:  true,
		PgpEnvelopeYcSaKeyFileSetting: true,
		PgpEnvelopeYcEndpointSetting:  true,
		LibsodiumKeySetting:           true,
		LibsodiumKeyPathSetting:       true,
		LibsodiumKeyTransform:         true,
		TotalBgUploadedLimit:          true,
		NameStreamCreateCmd:           true,
		NameStreamRestoreCmd:          true,
		UseReverseUnpackSetting:       true,
		SkipRedundantTarsSetting:      true,
		VerifyPageChecksumsSetting:    true,
		StoreAllCorruptBlocksSetting:  true,
		UseRatingComposerSetting:      true,
		UseCopyComposerSetting:        true,
		UseDatabaseComposerSetting:    true,
		WithoutFilesMetadataSetting:   true,
		MaxDelayedSegmentsCount:       true,
		DeltaFromNameSetting:          true,
		DeltaFromUserDataSetting:      true,
		FetchTargetUserDataSetting:    true,
		SerializerTypeSetting:         true,
		StatsdAddressSetting:          true,
		StatsdExtraTagsSetting:        true,

		ProfileSamplingRatio: true,
		ProfileMode:          true,
		ProfilePath:          true,

		// Swift
		"WALG_SWIFT_PREFIX": true,
		SwiftOsAuthURL:      true,
		SwiftOsUsername:     true,
		SwiftOsPassword:     true,
		SwiftOsTenantName:   true,
		SwiftOsRegionName:   true,

		// AWS s3
		"WALG_S3_PREFIX":              true,
		"WALE_S3_PREFIX":              true,
		AwsAccessKeyID:                true,
		AwsSecretAccessKey:            true,
		AwsSessionToken:               true,
		"AWS_DEFAULT_REGION":          true,
		"AWS_DEFAULT_OUTPUT":          true,
		"AWS_PROFILE":                 true,
		"AWS_ROLE_ARN":                true,
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
		"WALG_S3_SSE_C":               true,
		"WALG_S3_SSE_KMS_ID":          true,
		"WALG_CSE_KMS_ID":             true,
		"WALG_CSE_KMS_REGION":         true,
		"WALG_S3_MAX_PART_SIZE":       true,
		"WALG_S3_ENDPOINT_SOURCE":     true,
		"WALG_S3_ENDPOINT_PORT":       true,
		"WALG_S3_USE_LIST_OBJECTS_V1": true,
		"WALG_S3_LOG_LEVEL":           true,
		"WALG_S3_RANGE_BATCH_ENABLED": true,
		"WALG_S3_RANGE_MAX_RETRIES":   true,
		"WALG_S3_MAX_RETRIES":         true,

		// Azure
		"WALG_AZ_PREFIX":         true,
		AzureStorageAccount:      true,
		AzureStorageAccessKey:    true,
		AzureStorageSasToken:     true,
		AzureEnvironmentName:     true,
		"WALG_AZURE_BUFFER_SIZE": true,
		"WALG_AZURE_MAX_BUFFERS": true,

		// GS
		"WALG_GS_PREFIX":             true,
		GoogleApplicationCredentials: true,

		// Yandex Cloud
		YcSaKeyFileSetting: true,
		YcKmsKeyIDSetting:  true,

		// SH
		"WALG_SSH_PREFIX": true,
		SSHPort:           true,
		SSHPassword:       true,
		SSHUsername:       true,
		SSHPrivateKeyPath: true,

		//File
		"WALG_FILE_PREFIX": true,

		// GOLANG
		GoMaxProcs: true,

		// Web server
		HTTPListen:       true,
		HTTPExposePprof:  true,
		HTTPExposeExpVar: true,
	}

	PGAllowedSettings = map[string]bool{
		// Postgres
		PgPortSetting:                        true,
		PgUserSetting:                        true,
		PgHostSetting:                        true,
		PgDataSetting:                        true,
		PgPasswordSetting:                    true,
		PgPassfileSetting:                    true,
		PgDatabaseSetting:                    true,
		PgSslModeSetting:                     true,
		PgSslCert:                            true,
		PgSslKey:                             true,
		PgSslRootCert:                        true,
		PgSlotName:                           true,
		PgWalSize:                            true,
		PrefetchDir:                          true,
		PgReadyRename:                        true,
		PgBackRestStanza:                     true,
		PgAliveCheckInterval:                 true,
		PgStopBackupTimeout:                  true,
		FailoverStorages:                     true,
		FailoverStoragesCheckTimeout:         true,
		FailoverStorageCacheLifetime:         true,
		FailoverStorageCacheEMAAliveLimit:    true,
		FailoverStorageCacheEMADeadLimit:     true,
		FailoverStorageCacheEMAAlphaAliveMax: true,
		FailoverStorageCacheEMAAlphaAliveMin: true,
		FailoverStorageCacheEMAAlphaDeadMax:  true,
		FailoverStorageCacheEMAAlphaDeadMin:  true,
		FailoverStoragesCheckSize:            true,
		PgDaemonWALUploadTimeout:             true,
	}

	MongoAllowedSettings = map[string]bool{
		// MongoDB
		MongoDBUriSetting:                  true,
		MongoDBLastWriteUpdateInterval:     true,
		MongoDBExtendBackupCursor:          true,
		MongoDBDeletionProtectionWhitelist: true,
		OplogArchiveTimeoutInterval:        true,
		OplogArchiveAfterSize:              true,
		OplogPushStatsEnabled:              true,
		OplogPushStatsLoggingInterval:      true,
		OplogPushStatsUpdateInterval:       true,
		OplogPushStatsExposeHTTP:           true,
		OplogPushWaitForBecomePrimary:      true,
		OplogPushPrimaryCheckInterval:      true,
		OplogPITRDiscoveryInterval:         true,
		StreamSplitterBlockSize:            true,
		StreamSplitterPartitions:           true,
	}

	SQLServerAllowedSettings = map[string]bool{
		// SQLServer
		SQLServerBlobHostname:     true,
		SQLServerBlobCertFile:     true,
		SQLServerBlobKeyFile:      true,
		SQLServerBlobLockFile:     true,
		SQLServerConnectionString: true,
		SQLServerDBConcurrency:    true,
		SQLServerReuseProxy:       true,
	}

	MysqlAllowedSettings = map[string]bool{
		// MySQL
		MysqlDatasourceNameSetting:     true,
		MysqlSslCaSetting:              true,
		MysqlBinlogReplayCmd:           true,
		MysqlBinlogDstSetting:          true,
		MysqlBackupPrepareCmd:          true,
		MysqlTakeBinlogsFromMaster:     true,
		MysqlCheckGTIDs:                true,
		StreamSplitterPartitions:       true,
		StreamSplitterBlockSize:        true,
		StreamSplitterMaxFileSize:      true,
		MysqlBinlogServerHost:          true,
		MysqlBinlogServerPort:          true,
		MysqlBinlogServerUser:          true,
		MysqlBinlogServerPassword:      true,
		MysqlBinlogServerID:            true,
		MysqlBinlogServerReplicaSource: true,
		MysqlBackupDownloadMaxRetry:    true,
		MysqlIncrementalBackupDst:      true,
		MysqlDataDir:                   true,
	}

	RedisAllowedSettings = map[string]bool{
		// Redis
		RedisUsername:            true,
		RedisPassword:            true,
		RedisDataPath:            true,
		RedisAppendonlyFolder:    true,
		RedisAppendonlyManifest:  true,
		RedisAppendonlyTmpFolder: true,
		RedisDataThreshold:       true,
		RedisDataTimeout:         true,
		RedisServerProcessName:   true,
	}

	GPAllowedSettings = map[string]bool{
		GPLogsDirectory:                      true,
		GPSegContentID:                       true,
		GPSegmentsPollRetries:                true,
		GPSegmentsPollInterval:               true,
		GPSegmentsUpdInterval:                true,
		GPSegmentStatesDir:                   true,
		GPDeleteConcurrency:                  true,
		GPAoSegSizeThreshold:                 true,
		GPAoDeduplicationAgeLimit:            true,
		GPRelativeRecoveryConfPath:           true,
		GPRelativePostgresqlConfPath:         true,
		FailoverStorages:                     true,
		FailoverStoragesCheckTimeout:         true,
		FailoverStorageCacheLifetime:         true,
		FailoverStorageCacheEMAAliveLimit:    true,
		FailoverStorageCacheEMADeadLimit:     true,
		FailoverStorageCacheEMAAlphaAliveMax: true,
		FailoverStorageCacheEMAAlphaAliveMin: true,
		FailoverStorageCacheEMAAlphaDeadMax:  true,
		FailoverStorageCacheEMAAlphaDeadMin:  true,
		FailoverStoragesCheckSize:            true,
	}

	RequiredSettings       = make(map[string]bool)
	HTTPSettingExposeFuncs = map[string]func(webserver.WebServer){
		HTTPExposePprof:          webserver.EnablePprofEndpoints,
		HTTPExposeExpVar:         webserver.EnableExpVarEndpoints,
		OplogPushStatsExposeHTTP: nil,
	}
	Turbo bool

	secretSettings = map[string]bool{
		"WALE_" + GpgKeyIDSetting:    true,
		"WALG_" + GpgKeyIDSetting:    true,
		AwsAccessKeyID:               true,
		AwsSecretAccessKey:           true,
		AwsSessionToken:              true,
		AzureStorageAccessKey:        true,
		AzureStorageSasToken:         true,
		GoogleApplicationCredentials: true,
		LibsodiumKeySetting:          true,
		PgPasswordSetting:            true,
		PgpKeyPassphraseSetting:      true,
		PgpKeySetting:                true,
		PgpEnvelopeKeySetting:        true,
		RedisUsername:                true,
		RedisPassword:                true,
		SQLServerConnectionString:    true,
		SSHPassword:                  true,
		SwiftOsPassword:              true,
	}

	complexSettings = map[string]bool{
		FailoverStorages:       true,
		StatsdExtraTagsSetting: true,
	}
)

const MinAllowedConcurrency = 1

type InvalidConcurrencyValueError struct {
	error
}

func newInvalidConcurrencyValueError(concurrencyType string, value int) InvalidConcurrencyValueError {
	return InvalidConcurrencyValueError{
		errors.Errorf("%v value is expected to be positive but is: %v",
			concurrencyType, value)}
}

func (err InvalidConcurrencyValueError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnsetRequiredSettingError struct {
	error
}

func NewUnsetRequiredSettingError(settingName string) UnsetRequiredSettingError {
	return UnsetRequiredSettingError{errors.Errorf("%v is required to be set, but it isn't", settingName)}
}

func (err UnsetRequiredSettingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func AddTurboFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVarP(&Turbo, "turbo", "", false,
		"Ignore all kinds of throttling defined in config")
}

func isAllowedSetting(setting string, AllowedSettings map[string]bool) (exists bool) {
	_, exists = AllowedSettings[setting]
	return
}

// GetSetting extract setting by key if key is set, return empty string otherwise
func GetSetting(key string) (value string, ok bool) {
	if viper.IsSet(key) {
		value := viper.GetString(key)
		value = strings.TrimRight(value, "\r\n")
		return value, true
	}
	return "", false
}

func GetWaleCompatibleSetting(key string) (value string, exists bool) {
	return GetWaleCompatibleSettingFrom(key, viper.GetViper())
}

func GetWaleCompatibleSettingFrom(key string, config *viper.Viper) (value string, exists bool) {
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
		if val, ok := DefaultConfigValues[settingKey]; ok && len(val) > 0 {
			return val, true
		}
	}
	return "", false
}

func ConfigureLogging() error {
	var logFile *os.File
	logLevel := CommonDefaultConfigValues[LogLevelSetting]
	var err error

	if viper.IsSet(LogLevelSetting) {
		logLevel = viper.GetString(LogLevelSetting)
	}

	if viper.IsSet(LogDestinationSetting) && viper.GetString(LogDestinationSetting) != "stderr" {
		logFileName := viper.GetString(LogDestinationSetting)
		logFile, err = os.OpenFile(logFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("could not open log file: %s", err)
		}
	} else {
		logFile = os.Stderr
	}

	if err := tracelog.Setup(logFile, logLevel); err != nil {
		return fmt.Errorf("failed to setup logging: %s", err)
	}

	if logging.LogFile != nil {
		_ = logging.LogFile.Close()
	}
	if logFile != os.Stderr {
		logging.LogFile = logFile
	}
	return nil
}

func Configure() {
	err := ConfigureLogging()
	if err != nil {
		tracelog.ErrorLogger.Println("Failed to configure logging.")
		tracelog.ErrorLogger.FatalError(err)
	}

	// Show all relevant ENV vars in DEVEL Logging Mode
	{
		var buff bytes.Buffer
		buff.WriteString("--- COMPILED ENVIRONMENT VARS ---\n")

		var keys []string
		for k := range viper.AllSettings() {
			keys = append(keys, strings.ToUpper(k))
		}
		sort.Strings(keys)

		for _, k := range keys {
			val, ok := os.LookupEnv(k)
			if !ok {
				continue
			}

			// for secret settings: leave them empty if they are defined but empty, otherwise hide their actual value
			if secretSettings[k] && val != "" {
				val = "--HIDDEN--"
			}
			fmt.Fprintf(&buff, "\t%s=%s\n", k, val)
		}

		tracelog.DebugLogger.Print(buff.String())
	}
}

func SetupSignalListener() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)
	go func() {
		for {
			<-sigCh
			if err := ConfigureLogging(); err != nil {
				tracelog.ErrorLogger.Printf("error configuring logging: %s\n", err.Error())
			}
		}
	}()
}

// ConfigureAndRunDefaultWebServer configures and runs web server
func ConfigureAndRunDefaultWebServer() error {
	var ws webserver.WebServer
	httpListenAddr, httpListen := GetSetting(HTTPListen)
	if httpListen {
		ws = webserver.NewSimpleWebServer(httpListenAddr)
		if err := ws.Serve(); err != nil {
			return err
		}
		if err := webserver.SetDefaultWebServer(ws); err != nil {
			return err
		}
	}
	for setting, registerFunc := range HTTPSettingExposeFuncs {
		enabled, err := GetBoolSettingDefault(setting, false)
		if err != nil {
			return err
		}
		if !enabled {
			continue
		}
		if !httpListen {
			return fmt.Errorf("%s failed: %s is not set", setting, HTTPListen)
		}
		if registerFunc == nil {
			continue
		}
		registerFunc(ws)
	}
	return nil
}

func AddConfigFlags(Cmd *cobra.Command, hiddenCfgFlagAnnotation string) {
	cfgFlags := &pflag.FlagSet{}
	for k := range AllowedSettings {
		flagName := ToFlagName(k)
		isRequired, exist := RequiredSettings[k]
		flagUsage := ""
		if exist && isRequired {
			flagUsage = "Required, can be set though this flag or " + k + " variable"
		}

		cfgFlags.String(flagName, "", flagUsage)
		_ = viper.BindPFlag(k, cfgFlags.Lookup(flagName))
	}
	cfgFlags.VisitAll(func(f *pflag.Flag) {
		if f.Annotations == nil {
			f.Annotations = map[string][]string{}
		}
		f.Annotations[hiddenCfgFlagAnnotation] = []string{"true"}
	})
	Cmd.PersistentFlags().AddFlagSet(cfgFlags)
}

// InitConfig reads config file and ENV variables if set.
func InitConfig() {
	var globalViper = viper.GetViper()
	globalViper.AutomaticEnv() // read in environment variables that match
	SetDefaultValues(globalViper)
	SetGoMaxProcs(globalViper)
	ReadConfigFromFile(globalViper, CfgFile)
	CheckAllowedSettings(globalViper)

	bindConfigToEnv(globalViper)
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
	for setting, value := range DefaultConfigValues {
		config.SetDefault(setting, value)
	}
}

func SetGoMaxProcs(config *viper.Viper) {
	gomaxprocs := config.GetInt(GoMaxProcs)
	if !Turbo && gomaxprocs > 0 {
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

func ToFlagName(s string) string {
	return strings.ReplaceAll(strings.ToLower(s), "_", "-")
}

// Set the compiled config to ENV.
// Applicable for Swift/Postgres/etc libs that waiting config paramenters only from ENV.
func bindConfigToEnv(globalViper *viper.Viper) {
	for k, v := range globalViper.AllSettings() {
		val := fmt.Sprint(v)
		k = strings.ToUpper(k)

		// avoid filling environment with empty values :
		// if val is empty, and os.Getenv(k) is also empty (<- can be because the env variable is not set),
		// we don't create an env variable at all
		if val == "" && os.Getenv(k) == "" {
			continue
		}

		if complexSettings[k] {
			continue
		}

		err := os.Setenv(k, val)
		if err != nil {
			err = errors.Wrap(err, "Failed to bind config to env variable")
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}
}

func GetRequiredSetting(setting string) (string, error) {
	val, ok := GetSetting(setting)
	if !ok {
		return "", NewUnsetRequiredSettingError(setting)
	}
	return val, nil
}

func GetBoolSettingDefault(setting string, def bool) (bool, error) {
	val, ok := GetSetting(setting)
	if !ok {
		return def, nil
	}
	return strconv.ParseBool(val)
}

func GetBoolSetting(setting string) (val bool, ok bool, err error) {
	valstr, ok := GetSetting(setting)
	if !ok {
		return false, false, nil
	}
	val, err = strconv.ParseBool(valstr)
	return val, true, err
}

func GetFloatSettingDefault(setting string, def float64) (float64, error) {
	val, ok := GetSetting(setting)
	if !ok {
		return def, nil
	}
	return strconv.ParseFloat(val, 64)
}

func GetFloatSetting(setting string) (val float64, ok bool, err error) {
	valstr, ok := GetSetting(setting)
	if !ok {
		return 0, false, nil
	}
	val, err = strconv.ParseFloat(valstr, 64)
	return val, true, err
}

func GetMaxUploadDiskConcurrency() (int, error) {
	if Turbo {
		return 4, nil
	}
	return GetMaxConcurrency(UploadDiskConcurrencySetting)
}

func GetMaxConcurrency(concurrencyType string) (int, error) {
	concurrency := viper.GetInt(concurrencyType)

	if concurrency < MinAllowedConcurrency {
		return MinAllowedConcurrency, newInvalidConcurrencyValueError(concurrencyType, concurrency)
	}
	return concurrency, nil
}

func GetMaxDownloadConcurrency() (int, error) {
	return GetMaxConcurrency(DownloadConcurrencySetting)
}

func GetMaxUploadConcurrency() (int, error) {
	return GetMaxConcurrency(UploadConcurrencySetting)
}

// This setting is intentionally undocumented in README. Effectively, this configures how many prepared tar Files there
// may be in uploading state during backup-push.
func GetMaxUploadQueue() (int, error) {
	return GetMaxConcurrency(UploadQueueSetting)
}

func GetFetchRetries() int {
	concurrency := viper.GetInt(DownloadFileRetriesSetting)
	return concurrency
}

func GetDurationSetting(setting string) (time.Duration, error) {
	intervalStr, ok := GetSetting(setting)
	if !ok {
		return 0, NewUnsetRequiredSettingError(setting)
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return 0, fmt.Errorf("duration expected for %s setting but given '%s': %w", setting, intervalStr, err)
	}
	return interval, nil
}

func GetDurationSettingDefault(setting string, def time.Duration) (time.Duration, error) {
	intervalStr, ok := GetSetting(setting)
	if !ok {
		return def, nil
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return 0, fmt.Errorf("duration expected for %s setting but given '%s': %w", setting, intervalStr, err)
	}
	return interval, nil
}

func GetOplogPITRDiscoveryIntervalSetting() (*time.Duration, error) {
	durStr, ok := GetSetting(OplogPITRDiscoveryInterval)
	if !ok {
		return nil, nil
	}
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		return nil, fmt.Errorf("duration expected for %s setting but given '%s': %w", OplogPITRDiscoveryInterval, durStr, err)
	}
	return &dur, nil
}
