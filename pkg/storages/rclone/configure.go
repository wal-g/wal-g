package rclone

import (
	"context"
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage/setting"
)

const (
	remoteNameSetting         = "RCLONE_REMOTE"
	configPathSetting         = "RCLONE_CONFIG_PATH"
	rcloneBinarySetting       = "RCLONE_BINARY_PATH"
	extraArgsSetting          = "RCLONE_EXTRA_ARGS"
	transfersSetting          = "RCLONE_TRANSFERS"
	bufferSizeSetting         = "RCLONE_BUFFER_SIZE"
	timeoutSetting            = "RCLONE_TIMEOUT"
	retriesSetting            = "RCLONE_RETRIES"
	lowLevelRetriesSetting    = "RCLONE_LOW_LEVEL_RETRIES"
	s3ChunkSizeSetting        = "RCLONE_S3_CHUNK_SIZE"
	uploadConcurrencySetting  = "RCLONE_UPLOAD_CONCURRENCY"
)

var SettingList = []string{
	remoteNameSetting,
	configPathSetting,
	rcloneBinarySetting,
	extraArgsSetting,
	transfersSetting,
	bufferSizeSetting,
	timeoutSetting,
	retriesSetting,
	lowLevelRetriesSetting,
	s3ChunkSizeSetting,
	uploadConcurrencySetting,
}

const (
	defaultRcloneBinary      = "rclone"
	defaultTransfers         = 4
	defaultBufferSize        = 16 * 1024 * 1024 // 16MB
	defaultTimeout           = 300              // 5 minutes
	defaultRetries           = 3
	defaultLowLevelRetries   = 10
)

func ConfigureStorage(
	_ context.Context,
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	remoteName, ok := settings[remoteNameSetting]
	if !ok || remoteName == "" {
		return nil, fmt.Errorf("required setting %s is not set", remoteNameSetting)
	}

	bucket, rootPath, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("extract bucket and path from prefix %q: %w", prefix, err)
	}

	rcloneBinary := defaultRcloneBinary
	if binary, ok := settings[rcloneBinarySetting]; ok && binary != "" {
		rcloneBinary = binary
	}

	transfers, err := setting.IntOptional(settings, transfersSetting, defaultTransfers)
	if err != nil {
		return nil, err
	}

	bufferSize, err := setting.IntOptional(settings, bufferSizeSetting, defaultBufferSize)
	if err != nil {
		return nil, err
	}

	timeout, err := setting.IntOptional(settings, timeoutSetting, defaultTimeout)
	if err != nil {
		return nil, err
	}

	retries, err := setting.IntOptional(settings, retriesSetting, defaultRetries)
	if err != nil {
		return nil, err
	}

	lowLevelRetries, err := setting.IntOptional(settings, lowLevelRetriesSetting, defaultLowLevelRetries)
	if err != nil {
		return nil, err
	}

	s3ChunkSize, err := setting.IntOptional(settings, s3ChunkSizeSetting, 0)
	if err != nil {
		return nil, err
	}

	uploadConcurrency, err := setting.IntOptional(settings, uploadConcurrencySetting, 0)
	if err != nil {
		return nil, err
	}

	config := &Config{
		RemoteName:        remoteName,
		Bucket:            bucket,
		RootPath:          rootPath,
		ConfigPath:        settings[configPathSetting],
		RcloneBinary:      rcloneBinary,
		ExtraArgs:         settings[extraArgsSetting],
		Transfers:         transfers,
		BufferSize:        bufferSize,
		Timeout:           timeout,
		Retries:           retries,
		LowLevelRetries:   lowLevelRetries,
		S3ChunkSize:       s3ChunkSize,
		UploadConcurrency: uploadConcurrency,
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create Rclone storage: %w", err)
	}
	return st, nil
}
