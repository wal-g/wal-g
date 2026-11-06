package gcs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"google.golang.org/api/option"
)

type prefixContextKey struct{}

type prefixTransport func(*http.Request) (*http.Response, error)

func (f prefixTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestListObjectsWithPrefix(t *testing.T) {
	for _, scenario := range []string{"pages", "empty", "first failure", "later failure", "literal"} {
		t.Run(scenario, func(t *testing.T) {
			prefix, fullPrefix := "0000000800000002", "archive/wal_005/0000000800000002"
			if scenario == "literal" {
				prefix, fullPrefix = "a*?[x]//../", "archive/wal_005/a*?[x]//../"
			}
			ctx := context.WithValue(context.Background(), prefixContextKey{}, "listing-context")
			calls := 0
			client, err := gcs.NewClient(context.Background(), option.WithHTTPClient(&http.Client{Transport: prefixTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, "listing-context", r.Context().Value(prefixContextKey{}))
				require.LessOrEqual(t, calls, 2)
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "/storage/v1/b/bucket/o", r.URL.Path)
				q := r.URL.Query()
				require.Equal(t, fullPrefix, q.Get("prefix"))
				require.Empty(t, q.Get("delimiter"))
				fields := strings.FieldsFunc(q.Get("fields"), func(r rune) bool {
					return r == ',' || r == '(' || r == ')'
				})
				require.ElementsMatch(t, []string{"nextPageToken", "prefixes", "items", "name", "size", "updated"}, fields)
				if calls == 1 {
					require.Empty(t, q.Get("pageToken"))
				} else {
					require.Equal(t, "next-page", q.Get("pageToken"))
				}
				status, body := http.StatusOK, `{}`
				switch {
				case scenario == "first failure" || calls == 2 && scenario == "later failure":
					status, body = http.StatusForbidden, `{"error":{"code":403,"message":"denied"}}`
				case scenario != "empty":
					next := ""
					if calls == 1 {
						next = "next-page"
					}
					body = fmt.Sprintf(`{"nextPageToken":%q,"items":[{"name":%q,"size":"123","updated":"2026-10-06T12:00:00Z"}]}`, next, fmt.Sprintf("%s%024d.lz4", fullPrefix, calls))
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			folder := NewFolder(client.Bucket("bucket"), "archive/wal_005", nil, &Config{ContextTimeout: time.Minute})
			lister, ok := interface{}(folder).(storage.PrefixLister)
			require.True(t, ok, "GCS must support native prefix listing")
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
						require.Equal(t, time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC), obj.GetLastModified())
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
	for _, normalize := range []bool{false, true} {
		for _, tc := range []struct {
			root       string
			wantPrefix string
		}{
			{"archive", "archive/wal_005/0000000800000002"},
			{"archive/", "archive/wal_005/0000000800000002"},
			{"", "wal_005/0000000800000002"},
		} {
			t.Run(fmt.Sprintf("normalize=%t/root=%q", normalize, tc.root), func(t *testing.T) {
				calls := 0
				transport := prefixTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					require.Equal(t, tc.wantPrefix, r.URL.Query().Get("prefix"))
					body := fmt.Sprintf(`{"items":[{"name":%q,"size":"123","updated":"2026-10-06T12:00:00Z"}]}`,
						tc.wantPrefix+"00000001.lz4")
					return &http.Response{
						StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
						Body: io.NopCloser(strings.NewReader(body)), Request: r,
					}, nil
				})
				client, err := gcs.NewClient(context.Background(), option.WithHTTPClient(&http.Client{Transport: transport}))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, client.Close()) })
				root := NewFolder(client.Bucket("bucket"), tc.root, nil, &Config{
					NormalizePrefix: normalize, ContextTimeout: time.Minute,
				})
				objects, err := storage.ListObjectsWithPrefix(context.Background(), root.GetSubFolder("wal_005/"), "0000000800000002")
				require.NoError(t, err)
				require.Len(t, objects, 1)
				require.Equal(t, "000000080000000200000001.lz4", objects[0].GetName())
				require.Equal(t, 1, calls)
			})
		}
	}
}

func TestJoinPathEmptyComponents(t *testing.T) {
	folder := &Folder{config: &Config{NormalizePrefix: false}}
	for _, tc := range []struct{ root, relative, want string }{
		{"", "wal_005/", "wal_005/"},
		{"archive/", "", "archive/"},
		{"", "", ""},
		{"archive//", "/wal_005/", "archive//wal_005/"},
	} {
		t.Run(fmt.Sprintf("%q+%q", tc.root, tc.relative), func(t *testing.T) {
			require.Equal(t, tc.want, folder.joinPath(tc.root, tc.relative))
		})
	}
}
