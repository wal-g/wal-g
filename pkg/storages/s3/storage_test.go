package s3

import (
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestS3FolderCreatesWithoutAdditionalHeaders(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
		})

	assert.NoError(t, err)
}

func TestS3FolderCreatesWithAdditionalHeadersJSON(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
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
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
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
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
		})

	assert.NoError(t, err)
}

func TestS3FolderRequestTimeoutAcceptsValue(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
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
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
			requestTimeoutSetting:    "not-a-number",
		})

	assert.Error(t, err)
}

func TestBuildHTTPClientAppliesRequestTimeout(t *testing.T) {
	client, err := buildHTTPClient(&Config{RequestTimeout: 30 * time.Second}, "")
	require.NoError(t, err)

	transport := underlyingTransport(t, client)
	assert.Equal(t, 30*time.Second, transport.ResponseHeaderTimeout)
}

func TestBuildHTTPClientLeavesTimeoutZeroWhenUnset(t *testing.T) {
	client, err := buildHTTPClient(&Config{}, "")
	require.NoError(t, err)

	transport := underlyingTransport(t, client)
	assert.Equal(t, time.Duration(0), transport.ResponseHeaderTimeout)
}

func underlyingTransport(t *testing.T, client aws.HTTPClient) *http.Transport {
	t.Helper()
	httpClient, ok := client.(*http.Client)
	require.True(t, ok, "expected *http.Client")
	wrapped, ok := httpClient.Transport.(*loggingTransport)
	require.True(t, ok, "expected loggingTransport wrapper")
	transport, ok := wrapped.underlying.(*http.Transport)
	require.True(t, ok, "expected underlying *http.Transport")
	return transport
}

func TestS3Folder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting: "HTTP://s3.kek.lol.net/",
		})
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}
func TestS3FolderEndpointSource(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(t.Context(), waleS3Prefix,
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
