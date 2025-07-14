package oss

import (
	"fmt"
	"strings"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage/setting"
)

const (
	// using OSS_ prefix since alicloud credentials package
	// is following the same convention
	accessKeyIDSetting     = "OSS_ACCESS_KEY_ID"
	accessKeySecretSetting = "OSS_ACCESS_KEY_SECRET"
	securityTokenSetting   = "OSS_SESSION_TOKEN"
	regionSetting          = "OSS_REGION"
	roleARNSetting         = "OSS_ROLE_ARN"
	roleSessionNameSetting = "OSS_ROLE_SESSION_NAME"
	skipValidationSetting  = "OSS_SKIP_VALIDATION"
	maxRetriesSetting      = "OSS_MAX_RETRIES"
)

var SettingList = []string{
	accessKeyIDSetting,
	accessKeySecretSetting,
	securityTokenSetting,
	regionSetting,
	roleARNSetting,
	roleSessionNameSetting,
	skipValidationSetting,
	maxRetriesSetting,
}

const (
	defaultSkipValidation = false
	defaultMaxRetries     = 15
	defaultLogLevel       = "info"
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

	config := &Config{
		AccessKeyId:     strings.TrimSpace(settings[accessKeyIDSetting]),
		AccessKeySecret: strings.TrimSpace(settings[accessKeySecretSetting]),
		SecurityToken:   strings.TrimSpace(settings[securityTokenSetting]),
		RoleARN:         strings.TrimSpace(settings[roleARNSetting]),
		RoleSessionName: strings.TrimSpace(settings[roleSessionNameSetting]),
		Bucket:          bucket,
		RootPath:        rootPath,
		SkipValidation:  skipValidation,
		MaxRetries:      maxRetries,
		Region:          settings[regionSetting],
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create storage: %w", err)
	}
	return st, nil
}
