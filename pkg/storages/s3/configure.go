package s3

import (
	"fmt"
	"strings"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage/setting"
)

// TODO: Merge the settings and their default values with ones defined in internal/config.go

const (
	endpointSetting                 = "AWS_ENDPOINT"
	regionSetting                   = "AWS_REGION"
	forcePathStyleSetting           = "AWS_S3_FORCE_PATH_STYLE"
	accessKeyIDSetting              = "AWS_ACCESS_KEY_ID"
	accessKeySetting                = "AWS_ACCESS_KEY"
	secretAccessKeySetting          = "AWS_SECRET_ACCESS_KEY"
	secretKeySetting                = "AWS_SECRET_KEY"
	sessionTokenSetting             = "AWS_SESSION_TOKEN"
	sessionNameSetting              = "AWS_ROLE_SESSION_NAME"
	roleARNSetting                  = "AWS_ROLE_ARN"
	dualStackSetting                = "AWS_DUAL_STACK"
	skipValidationSetting           = "S3_SKIP_VALIDATION"
	useYcSessionTokenSetting        = "S3_USE_YC_SESSION_TOKEN"
	sseSetting                      = "S3_SSE"
	sseCSetting                     = "S3_SSE_C"
	sseKmsIDSetting                 = "S3_SSE_KMS_ID"
	storageClassSetting             = "S3_STORAGE_CLASS"
	uploadConcurrencySetting        = "UPLOAD_CONCURRENCY"
	caCertFileSetting               = "S3_CA_CERT_FILE"
	maxPartSizeSetting              = "S3_MAX_PART_SIZE"
	endpointSourceSetting           = "S3_ENDPOINT_SOURCE"
	endpointPortSetting             = "S3_ENDPOINT_PORT"
	logLevelSetting                 = "S3_LOG_LEVEL"
	useListObjectsV1Setting         = "S3_USE_LIST_OBJECTS_V1"
	rangeBatchEnabledSetting        = "S3_RANGE_BATCH_ENABLED"
	rangeQueriesMaxRetriesSetting   = "S3_RANGE_MAX_RETRIES"
	requestAdditionalHeadersSetting = "S3_REQUEST_ADDITIONAL_HEADERS"
	retentionPeriodSetting          = "S3_RETENTION_PERIOD"
	retentionModeSetting            = "S3_RETENTION_MODE"
	// limiters for retry policy during interaction with S3
	maxRetriesSetting              = "S3_MAX_RETRIES"
	minThrottlingRetryDelaySetting = "S3_MIN_THROTTLING_RETRY_DELAY"
	maxThrottlingRetryDelaySetting = "S3_MAX_THROTTLING_RETRY_DELAY"
	disable100ContinueSetting      = "S3_DISABLE_100_CONTINUE"
)

var SettingList = []string{
	endpointPortSetting,
	endpointSetting,
	endpointSourceSetting,
	regionSetting,
	forcePathStyleSetting,
	accessKeyIDSetting,
	accessKeySetting,
	secretAccessKeySetting,
	secretKeySetting,
	sessionTokenSetting,
	sessionNameSetting,
	roleARNSetting,
	dualStackSetting,
	skipValidationSetting,
	useYcSessionTokenSetting,
	sseSetting,
	sseCSetting,
	sseKmsIDSetting,
	storageClassSetting,
	uploadConcurrencySetting,
	caCertFileSetting,
	maxPartSizeSetting,
	useListObjectsV1Setting,
	logLevelSetting,
	rangeBatchEnabledSetting,
	rangeQueriesMaxRetriesSetting,
	maxRetriesSetting,
	requestAdditionalHeadersSetting,
	minThrottlingRetryDelaySetting,
	maxThrottlingRetryDelaySetting,
	retentionPeriodSetting,
	retentionModeSetting,
	disable100ContinueSetting,
}

const (
	defaultPort                    = "443"
	defaultDualStack               = false
	defaultSkipValidation          = true
	defaultForcePathStyle          = false
	defaultUseListObjectsV1        = false
	defaultMaxRetries              = 15
	defaultMinThrottlingRetryDelay = 500
	defaultMaxThrottlingRetryDelay = 300000
	defaultMaxPartSize             = 20 << 20
	defaultStorageClass            = "STANDARD"
	defaultRangeBatchEnabled       = false
	defaultRangeMaxRetries         = 10
	defaultDisabledRetentionPeriod = -1
	defaultDisable100Continue      = false
)

// TODO: Unit tests
//
//nolint:funlen,gocyclo
func ConfigureStorage(
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	bucket, rootPath, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("extract bucket and path from prefix %q: %w", prefix, err)
	}

	port := defaultPort
	if p, ok := settings[endpointPortSetting]; ok {
		port = p
	}
	dualStack, err := setting.BoolOptional(settings, dualStackSetting, defaultDualStack)
	if err != nil {
		return nil, err
	}
	skipValidation, err := setting.BoolOptional(settings, skipValidationSetting, defaultSkipValidation)
	if err != nil {
		return nil, err
	}
	forcePathStyle, err := setting.BoolOptional(settings, forcePathStyleSetting, defaultForcePathStyle)
	if err != nil {
		return nil, err
	}
	useListObjectsV1, err := setting.BoolOptional(settings, useListObjectsV1Setting, defaultUseListObjectsV1)
	if err != nil {
		return nil, err
	}
	maxRetries, err := setting.IntOptional(settings, maxRetriesSetting, defaultMaxRetries)
	if err != nil {
		return nil, err
	}
	minThrottlingRetryDelay, err := setting.IntOptional(settings, minThrottlingRetryDelaySetting, defaultMinThrottlingRetryDelay)
	if err != nil {
		return nil, err
	}
	maxThrottlingRetryDelay, err := setting.IntOptional(settings, maxThrottlingRetryDelaySetting, defaultMaxThrottlingRetryDelay)
	if err != nil {
		return nil, err
	}
	uploadConcurrency, err := setting.Int(settings, uploadConcurrencySetting)
	if err != nil {
		return nil, err
	}
	maxPartSize, err := setting.IntOptional(settings, maxPartSizeSetting, defaultMaxPartSize)
	if err != nil {
		return nil, err
	}
	storageClass := defaultStorageClass
	if class, ok := settings[storageClassSetting]; ok {
		storageClass = class
	}
	rangeBatchEnabled, err := setting.BoolOptional(settings, rangeBatchEnabledSetting, defaultRangeBatchEnabled)
	if err != nil {
		return nil, err
	}
	rangeMaxRetries, err := setting.IntOptional(settings, rangeQueriesMaxRetriesSetting, defaultRangeMaxRetries)
	if err != nil {
		return nil, err
	}
	retentionPeriod, err := setting.IntOptional(settings, retentionPeriodSetting, defaultDisabledRetentionPeriod)
	if err != nil {
		return nil, err
	}
	disable100Continue, err := setting.BoolOptional(settings, disable100ContinueSetting, defaultDisable100Continue)
	if err != nil {
		return nil, err
	}

	config := &Config{
		Secrets: &Secrets{
			SecretKey: strings.TrimSpace(setting.FirstDefined(settings, secretAccessKeySetting, secretKeySetting)),
		},
		Region:                   settings[regionSetting],
		Endpoint:                 settings[endpointSetting],
		EndpointSource:           settings[endpointSourceSetting],
		EndpointPort:             port,
		Bucket:                   bucket,
		RootPath:                 rootPath,
		AccessKey:                strings.TrimSpace(setting.FirstDefined(settings, accessKeyIDSetting, accessKeySetting)),
		SessionToken:             settings[sessionTokenSetting],
		RoleARN:                  settings[roleARNSetting],
		DualStack:                dualStack,
		SessionName:              settings[sessionNameSetting],
		CACertFile:               settings[caCertFileSetting],
		SkipValidation:           skipValidation,
		UseYCSessionToken:        settings[useYcSessionTokenSetting],
		ForcePathStyle:           forcePathStyle,
		RequestAdditionalHeaders: settings[requestAdditionalHeadersSetting],
		UseListObjectsV1:         useListObjectsV1,
		MaxRetries:               maxRetries,
		LogLevel:                 settings[logLevelSetting],
		Uploader: &UploaderConfig{
			UploadConcurrency:            uploadConcurrency,
			MaxPartSize:                  maxPartSize,
			StorageClass:                 storageClass,
			ServerSideEncryption:         settings[sseSetting],
			ServerSideEncryptionCustomer: settings[sseCSetting],
			ServerSideEncryptionKMSID:    settings[sseKmsIDSetting],
			RetentionPeriod:              retentionPeriod,
			RetentionMode:                settings[retentionModeSetting],
		},
		RangeBatchEnabled:       rangeBatchEnabled,
		RangeMaxRetries:         rangeMaxRetries,
		MinThrottlingRetryDelay: time.Duration(minThrottlingRetryDelay) * time.Millisecond,
		MaxThrottlingRetryDelay: time.Duration(maxThrottlingRetryDelay) * time.Millisecond,
		Disable100Continue:      disable100Continue,
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create S3 storage: %w", err)
	}
	return st, nil
}
