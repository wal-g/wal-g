package gcs

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage/setting"
)

const (
	contextTimeoutSetting  = "GCS_CONTEXT_TIMEOUT"
	normalizePrefixSetting = "GCS_NORMALIZE_PREFIX"
	encryptionKeySetting   = "GCS_ENCRYPTION_KEY"
	maxChunkSizeSetting    = "GCS_MAX_CHUNK_SIZE"
	maxRetriesSetting      = "GCS_MAX_RETRIES"
)

// SettingList provides a list of GCS folder settings.
var SettingList = []string{
	contextTimeoutSetting,
	normalizePrefixSetting,
	encryptionKeySetting,
	maxChunkSizeSetting,
	maxRetriesSetting,
}

const (
	defaultNormalizePrefix = true
	defaultContextTimeout  = 60 * 60 // 1 hour

	// The maximum number of chunks cannot exceed 32.
	// So, increase the chunk size to 50 MiB to be able to upload files up to 1600 MiB.
	defaultMaxChunkSize = 50 << 20

	// defaultMaxRetries limits upload and download retries during interaction with GCS.
	defaultMaxRetries = 16

	encryptionKeySize = 32
)

// TODO: Unit tests
func ConfigureStorage(
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	normalizePrefix, err := setting.BoolOptional(settings, normalizePrefixSetting, defaultNormalizePrefix)
	if err != nil {
		return nil, err
	}

	var bucketName, path string
	if normalizePrefix {
		bucketName, path, err = storage.GetPathFromPrefix(prefix)
	} else {
		// Special mode for WAL-E compatibility with strange prefixes
		bucketName, path, err = storage.ParsePrefixAsURL(prefix)
		if err == nil && path[0] == '/' {
			path = path[1:]
		}
	}
	if err != nil {
		return nil, fmt.Errorf("extract bucket and path from prefix %q: %w", prefix, err)
	}
	path = storage.AddDelimiterToPath(path)

	contextTimeout, err := setting.IntOptional(settings, contextTimeoutSetting, defaultContextTimeout)
	if err != nil {
		return nil, err
	}

	encryptionKey := make([]byte, 0)
	if encodedCSEK, ok := settings[encryptionKeySetting]; ok {
		decodedKey, err := base64.StdEncoding.DecodeString(encodedCSEK)
		if err != nil {
			return nil, fmt.Errorf("decode base64 Customer Supplied Encryption Key: %w", err)
		}
		if len(decodedKey) != encryptionKeySize {
			return nil, fmt.Errorf("invalid Customer Supplied Encryption Key: expected to be 32-byte long")
		}
		encryptionKey = decodedKey
	}

	maxChunkSize, err := setting.Int64Optional(settings, maxChunkSizeSetting, defaultMaxChunkSize)
	if err != nil {
		return nil, err
	}

	maxRetries, err := setting.IntOptional(settings, maxRetriesSetting, defaultMaxRetries)
	if err != nil {
		return nil, err
	}

	config := &Config{
		Secrets: &Secrets{
			EncryptionKey: encryptionKey,
		},
		RootPath:        path,
		Bucket:          bucketName,
		NormalizePrefix: normalizePrefix,
		ContextTimeout:  time.Second * time.Duration(contextTimeout),
		Uploader: &UploaderConfig{
			MaxChunkSize: maxChunkSize,
			MaxRetries:   maxRetries,
		},
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create Google Cloud storage: %w", err)
	}
	return st, nil
}
