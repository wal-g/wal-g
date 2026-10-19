package s3

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestS3FolderResponseHeaderTimeoutDefaultIsDisabled(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
		})

	assert.NoError(t, err)
}

func TestS3FolderResponseHeaderTimeoutAcceptsValue(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting:              "HTTP://s3.kek.lol.net/",
			skipValidationSetting:        "true",
			uploadConcurrencySetting:     "1",
			responseHeaderTimeoutSetting: "45",
		})

	assert.NoError(t, err)
}

func TestS3FolderResponseHeaderTimeoutRejectsNonInteger(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting:              "HTTP://s3.kek.lol.net/",
			skipValidationSetting:        "true",
			uploadConcurrencySetting:     "1",
			responseHeaderTimeoutSetting: "not-a-number",
		})

	assert.Error(t, err)
}

func TestBuildHTTPClientLeavesTimeoutZeroWhenUnset(t *testing.T) {
	client, err := buildHTTPClient(&Config{}, "")
	require.NoError(t, err)

	transport := underlyingTransport(t, client)
	assert.Equal(t, time.Duration(0), transport.ResponseHeaderTimeout)
}

// Delayed headers time out after the body is written. A slow body, once
// headers have been flushed, does not.
func TestResponseHeaderTimeoutBoundsHeadersOnly(t *testing.T) {
	const headerTimeout = 250 * time.Millisecond
	delayedHeaders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		timer := time.NewTimer(headerTimeout * 8)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(delayedHeaders.Close)
	slowBody := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(headerTimeout * 3)
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(slowBody.Close)

	client, err := buildHTTPClient(&Config{ResponseHeaderTimeout: headerTimeout}, "")
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, delayedHeaders.URL, strings.NewReader("payload"))
	require.NoError(t, err)
	_, err = client.Do(req)
	require.ErrorContains(t, err, "timeout awaiting response headers")

	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, slowBody.URL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "ok", string(body))
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
