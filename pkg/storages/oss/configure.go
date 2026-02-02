package oss

import (
	"fmt"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage/setting"
)

const (
	// using OSS_ prefix since alicloud credentials package
	// is following the same convention
	accessKeyIDSetting     = "OSS_ACCESS_KEY_ID"
	accessKeySecretSetting = "OSS_ACCESS_KEY_SECRET"
	securityTokenSetting   = "OSS_SESSION_TOKEN"
	endpointSetting        = "OSS_ENDPOINT"
	regionSetting          = "OSS_REGION"
	roleARNSetting         = "OSS_ROLE_ARN"
	roleSessionNameSetting = "OSS_ROLE_SESSION_NAME"
	skipValidationSetting  = "OSS_SKIP_VALIDATION"
	maxRetriesSetting      = "OSS_MAX_RETRIES"
	connectTimeoutSetting  = "OSS_CONNECT_TIMEOUT"
	uploadPartSizeSetting  = "OSS_UPLOAD_PART_SIZE"
	copyPartSizeSetting    = "OSS_COPY_PART_SIZE"
)

var SettingList = []string{
	accessKeyIDSetting,
	accessKeySecretSetting,
	securityTokenSetting,
	endpointSetting,
	regionSetting,
	roleARNSetting,
	roleSessionNameSetting,
	skipValidationSetting,
	maxRetriesSetting,
	connectTimeoutSetting,
	uploadPartSizeSetting,
	copyPartSizeSetting,
}

const (
	defaultSkipValidation        = false
	defaultMaxRetries            = 5
	defaultLogLevel              = "info"
	defaultConnectTimeoutSeconds = 5
)

func ConfigureStorage(
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	bucket, rootPath, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("extract bucket and path from prefix %q: %w", prefix, err)
	}

	skipValidation, err := setting.BoolOptional(settings, skipValidationSetting, defaultSkipValidation)
	if err != nil {
		return nil, err
	}

	maxRetries, err := setting.IntOptional(settings, maxRetriesSetting, defaultMaxRetries)
	if err != nil {
		return nil, err
	}

	connectTimeout, err := setting.Int64Optional(settings, connectTimeoutSetting, defaultConnectTimeoutSeconds)
	if err != nil {
		return nil, err
	}

	uploadPartSize, err := setting.Int64Optional(settings, uploadPartSizeSetting, oss.DefaultUploadPartSize)
	if err != nil {
		return nil, err
	}

	copyPartSize, err := setting.Int64Optional(settings, copyPartSizeSetting, oss.DefaultCopyPartSize)
	if err != nil {
		return nil, err
	}

	config := &Config{
		AccessKeyID:     strings.TrimSpace(settings[accessKeyIDSetting]),
		AccessKeySecret: strings.TrimSpace(settings[accessKeySecretSetting]),
		SecurityToken:   strings.TrimSpace(settings[securityTokenSetting]),
		RoleARN:         strings.TrimSpace(settings[roleARNSetting]),
		RoleSessionName: strings.TrimSpace(settings[roleSessionNameSetting]),
		Endpoint:        strings.TrimSpace(settings[endpointSetting]),
		Bucket:          bucket,
		RootPath:        rootPath,
		SkipValidation:  skipValidation,
		MaxRetries:      maxRetries,
		Region:          settings[regionSetting],
		ConnectTimeout:  connectTimeout,
		UploadPartSize:  uploadPartSize,
		CopyPartSize:    copyPartSize,
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create storage: %w", err)
	}
	return st, nil
}
