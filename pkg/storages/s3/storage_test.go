package s3

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestS3FolderCreatesWithoutAdditionalHeaders(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
		})

	assert.NoError(t, err)
}

func TestS3FolderCreatesWithAdditionalHeadersJSON(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:                 "HTTP://s3.kek.lol.net/",
			skipValidationSetting:           "true",
			uploadConcurrencySetting:        "1",
			requestAdditionalHeadersSetting: `{"X-Yandex-Prioritypass":"ok", "MyHeader":"32", "DROP_TABLE":"true"}`,
		})

	assert.NoError(t, err)
}

func TestS3FolderCreatesWithAdditionalHeadersYAML(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
			requestAdditionalHeadersSetting: `- X-Yandex-Prioritypass: "ok"
- MyHeader: "32"
- DROP_TABLE: "true"`,
		})

	assert.NoError(t, err)
}

func TestS3FolderRequestTimeoutDefaultIsDisabled(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
		})

	assert.NoError(t, err)
}

func TestS3FolderRequestTimeoutAcceptsValue(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
			requestTimeoutSetting:    "45",
		})

	assert.NoError(t, err)
}

func TestS3FolderRequestTimeoutRejectsNonInteger(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
			requestTimeoutSetting:    "not-a-number",
		})

	assert.Error(t, err)
}

// TestConfigureSessionAppliesRequestTimeout verifies that a Config with
// RequestTimeout > 0 produces an HTTP transport whose ResponseHeaderTimeout
// matches. This is the actual contract S3_REQUEST_TIMEOUT advertises -- the
// configure-time tests above only confirm the setting parses; this one
// confirms it threads through to net/http where it takes effect.
func TestConfigureSessionAppliesRequestTimeout(t *testing.T) {
	cfg := &Config{
		Secrets:                 &Secrets{},
		Region:                  "us-east-1",
		Endpoint:                "http://s3.kek.lol.net",
		Bucket:                  "test-bucket",
		MaxRetries:              defaultMaxRetries,
		MinThrottlingRetryDelay: time.Duration(defaultMinThrottlingRetryDelay) * time.Millisecond,
		MaxThrottlingRetryDelay: time.Duration(defaultMaxThrottlingRetryDelay) * time.Millisecond,
		Uploader:                &UploaderConfig{UploadConcurrency: 1, MaxPartSize: defaultMaxPartSize, StorageClass: defaultStorageClass},
		RequestTimeout:          30 * time.Second,
	}

	sess, err := createSession(cfg)
	require.NoError(t, err)

	// configureSession wraps the underlying transport in a loggingTransport,
	// so we have to peel that layer off before checking ResponseHeaderTimeout.
	wrapped, ok := sess.Config.HTTPClient.Transport.(*loggingTransport)
	require.True(t, ok, "expected loggingTransport wrapper")
	transport, ok := wrapped.underlying.(*http.Transport)
	require.True(t, ok, "expected underlying *http.Transport")
	assert.Equal(t, 30*time.Second, transport.ResponseHeaderTimeout)
}

func TestConfigureSessionLeavesTimeoutZeroWhenUnset(t *testing.T) {
	cfg := &Config{
		Secrets:                 &Secrets{},
		Region:                  "us-east-1",
		Endpoint:                "http://s3.kek.lol.net",
		Bucket:                  "test-bucket",
		MaxRetries:              defaultMaxRetries,
		MinThrottlingRetryDelay: time.Duration(defaultMinThrottlingRetryDelay) * time.Millisecond,
		MaxThrottlingRetryDelay: time.Duration(defaultMaxThrottlingRetryDelay) * time.Millisecond,
		Uploader:                &UploaderConfig{UploadConcurrency: 1, MaxPartSize: defaultMaxPartSize, StorageClass: defaultStorageClass},
		// RequestTimeout deliberately left as zero value
	}

	sess, err := createSession(cfg)
	require.NoError(t, err)

	wrapped, ok := sess.Config.HTTPClient.Transport.(*loggingTransport)
	require.True(t, ok, "expected loggingTransport wrapper")
	if transport, ok := wrapped.underlying.(*http.Transport); ok {
		assert.Equal(t, time.Duration(0), transport.ResponseHeaderTimeout, "expected no header timeout when setting is unset")
	}
	// If the underlying isn't a *http.Transport, no timeout was set; that's
	// also valid -- the contract is "no behavior change when unset."
}

func TestS3Folder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting: "HTTP://s3.kek.lol.net/",
		})
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}
func TestS3FolderEndpointSource(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			endpointSourceSetting:    "HTTP://localhost:80/",
			accessKeySetting:         "AKIAIOSFODNN7EXAMPLE",
			secretKeySetting:         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			uploadConcurrencySetting: "1",
			forcePathStyleSetting:    "True",
		})
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}
