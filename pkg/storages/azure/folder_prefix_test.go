package azure

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type prefixContextKey struct{}

type prefixTransport func(*http.Request) (*http.Response, error)

func (f prefixTransport) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestListObjectsWithPrefix(t *testing.T) {
	for _, scenario := range []string{"pages", "empty", "first failure", "later failure", "literal"} {
		t.Run(scenario, func(t *testing.T) {
			prefix, fullPrefix := "0000000800000002", "archive/wal_005/0000000800000002"
			if scenario == "literal" {
				prefix, fullPrefix = "a*?[x]//../", "archive/wal_005/a*?[x]//../"
			}
			ctx := context.WithValue(context.Background(), prefixContextKey{}, "listing-context")
			calls := 0
			client, err := container.NewClientWithNoCredential("https://storage.example/bucket", &container.ClientOptions{ClientOptions: policy.ClientOptions{
				Retry: policy.RetryOptions{MaxRetries: -1},
				Transport: prefixTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					require.Equal(t, "listing-context", r.Context().Value(prefixContextKey{}))
					require.LessOrEqual(t, calls, 2)
					require.Equal(t, "GET", r.Method)
					require.Equal(t, "/bucket", r.URL.Path)
					q := r.URL.Query()
					require.Equal(t, "list", q.Get("comp"))
					require.Equal(t, "container", q.Get("restype"))
					require.Equal(t, fullPrefix, q.Get("prefix"))
					require.Empty(t, q.Get("delimiter"))
					require.Empty(t, q.Get("include"))
					if calls == 1 {
						require.Empty(t, q.Get("marker"))
					} else {
						require.Equal(t, "next-page", q.Get("marker"))
					}
					status, body := http.StatusOK, `<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`
					switch {
					case scenario == "first failure" || calls == 2 && scenario == "later failure":
						status, body = http.StatusForbidden, `<Error><Code>AuthorizationFailure</Code><Message>denied</Message></Error>`
					case scenario != "empty":
						next := ""
						if calls == 1 {
							next = "next-page"
						}
						body = fmt.Sprintf(`<EnumerationResults><Blobs><Blob><Name>%s%024d.lz4</Name><Properties><Last-Modified>Tue, 06 Oct 2026 12:00:00 GMT</Last-Modified><Content-Length>123</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob></Blobs><NextMarker>%s</NextMarker></EnumerationResults>`, fullPrefix, calls, next)
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/xml"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
				}),
			}})
			require.NoError(t, err)
			folder := NewFolder("archive/wal_005/", client, azblob.UploadStreamOptions{}, time.Minute)
			lister, ok := interface{}(folder).(storage.PrefixLister)
			require.True(t, ok, "Azure must support native prefix listing")
			objects, err := lister.ListObjectsWithPrefix(ctx, prefix)
			if strings.Contains(scenario, "failure") {
				require.Error(t, err)
				require.Empty(t, objects)
			} else {
				require.NoError(t, err)
				if scenario == "empty" {
					require.Empty(t, objects)
				} else {
					require.Len(t, objects, 2)
					for i, obj := range objects {
						require.Equal(t, fmt.Sprintf("%s%024d.lz4", prefix, i+1), obj.GetName())
						require.EqualValues(t, 123, obj.GetSize())
						require.Equal(t, time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC), obj.GetLastModified().UTC())
					}
				}
			}
			wantCalls := 2
			if scenario == "empty" || scenario == "first failure" {
				wantCalls = 1
			}
			require.Equal(t, wantCalls, calls)
		})
	}
}

func TestListObjectsWithPrefixInSubFolder(t *testing.T) {
	for _, tc := range []struct {
		root       string
		wantPrefix string
	}{
		{"archive", "archive/wal_005/0000000800000002"},
		{"archive/", "archive/wal_005/0000000800000002"},
		{"", "wal_005/0000000800000002"},
	} {
		t.Run(fmt.Sprintf("root=%q", tc.root), func(t *testing.T) {
			calls := 0
			transport := prefixTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, tc.wantPrefix, r.URL.Query().Get("prefix"))
				body := fmt.Sprintf(`<EnumerationResults><Blobs><Blob><Name>%s00000001.lz4</Name>`+
					`<Properties><Last-Modified>Tue, 06 Oct 2026 12:00:00 GMT</Last-Modified>`+
					`<Content-Length>123</Content-Length><BlobType>BlockBlob</BlobType></Properties>`+
					`</Blob></Blobs><NextMarker></NextMarker></EnumerationResults>`, tc.wantPrefix)
				return &http.Response{
					StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/xml"}},
					Body: io.NopCloser(strings.NewReader(body)), Request: r,
				}, nil
			})
			client, err := container.NewClientWithNoCredential("https://storage.example/bucket", &container.ClientOptions{
				ClientOptions: policy.ClientOptions{Transport: transport},
			})
			require.NoError(t, err)
			root := NewFolder(tc.root, client, azblob.UploadStreamOptions{}, time.Minute)
			objects, err := storage.ListObjectsWithPrefix(context.Background(), root.GetSubFolder("wal_005/"), "0000000800000002")
			require.NoError(t, err)
			require.Len(t, objects, 1)
			require.Equal(t, "000000080000000200000001.lz4", objects[0].GetName())
			require.Equal(t, 1, calls)
		})
	}
}
