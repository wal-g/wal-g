package gcs

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type objectErrorTransport struct {
	statusCode int
}

func (transport objectErrorTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body := fmt.Sprintf(`{"error":{"code":%d,"message":%q}}`,
		transport.statusCode, http.StatusText(transport.statusCode))
	return &http.Response{
		StatusCode: transport.statusCode,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func newObjectErrorBucket(t *testing.T, statusCode int) *gcs.BucketHandle {
	t.Helper()
	// JSON reads expose wrapped 404 errors; the SDK's XML read path returns the sentinel directly.
	client, err := gcs.NewClient(t.Context(),
		gcs.WithJSONReads(), gcs.WithDisabledClientMetrics(), option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{
			Transport: objectErrorTransport{statusCode: statusCode},
		}))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })
	return client.Bucket("test-bucket")
}

func assertPermissionDenied(t *testing.T, err error) {
	t.Helper()
	var apiError *googleapi.Error
	require.ErrorAs(t, err, &apiError)
	assert.Equal(t, http.StatusForbidden, apiError.Code)
	assert.NotErrorIs(t, err, gcs.ErrObjectNotExist)
	var notFound storage.ObjectNotFoundError
	assert.NotErrorAs(t, err, &notFound)
}

func TestGCSWrappedObjectNotExist(t *testing.T) {
	bucket := newObjectErrorBucket(t, http.StatusNotFound)
	_, err := bucket.Object("missing").Attrs(t.Context())
	require.Error(t, err)
	// The SDK wraps the sentinel; direct equality misses a real missing-object response.
	assert.False(t, err == gcs.ErrObjectNotExist)
	assert.ErrorIs(t, err, gcs.ErrObjectNotExist)
}

func TestGCSObjectErrors(t *testing.T) {
	for _, statusCode := range []int{http.StatusNotFound, http.StatusForbidden} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			bucket := newObjectErrorBucket(t, statusCode)
			folder := NewFolder(bucket, "prefix", nil, &Config{
				ContextTimeout:  time.Minute,
				NormalizePrefix: true,
			})

			t.Run("Exists", func(t *testing.T) {
				exists, err := folder.Exists(t.Context(), "missing")
				assert.False(t, exists)
				if statusCode == http.StatusNotFound {
					assert.NoError(t, err)
				} else {
					assertPermissionDenied(t, err)
				}
			})

			t.Run("StatObject", func(t *testing.T) {
				object, err := folder.StatObject(t.Context(), "missing")
				assert.Nil(t, object)
				if statusCode == http.StatusNotFound {
					assert.IsType(t, storage.ObjectNotFoundError{}, err)
				} else {
					assertPermissionDenied(t, err)
				}
			})

			t.Run("ReadObject", func(t *testing.T) {
				_, err := folder.ReadObject(t.Context(), "missing")
				if statusCode == http.StatusNotFound {
					assert.IsType(t, storage.ObjectNotFoundError{}, err)
				} else {
					assertPermissionDenied(t, err)
				}
			})

			t.Run("DeleteObjects", func(t *testing.T) {
				err := folder.DeleteObjects(t.Context(), []storage.Object{
					storage.NewLocalObject("missing", time.Time{}, 0),
				})
				if statusCode == http.StatusNotFound {
					assert.NoError(t, err)
				} else {
					assertPermissionDenied(t, err)
				}
			})

			t.Run("CleanUpChunks", func(t *testing.T) {
				object := bucket.Object("prefix/missing_chunks/chunk0")
				uploader := NewUploader(object, &UploaderConfig{})
				// Test the callback directly: CleanUpChunks logs errors instead of returning them.
				err := uploader.getCleanUpChunksFunc(object)(t.Context())
				if statusCode == http.StatusNotFound {
					assert.NoError(t, err)
				} else {
					assertPermissionDenied(t, err)
				}
			})
		})
	}
}
