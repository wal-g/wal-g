package s3_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type prefixContextKey struct{}

type prefixTransport func(*http.Request) (*http.Response, error)

func (f prefixTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestListObjectsWithPrefix(t *testing.T) {
	for _, scenario := range []string{"pages", "empty", "first failure", "later failure", "later not found", "first not found", "missing size", "literal"} {
		t.Run(scenario, func(t *testing.T) {
			prefix := "0000000800000002"
			fullPrefix := "archive/wal_005/0000000800000002"
			if scenario == "literal" {
				prefix = "a*?[x]//../"
				fullPrefix = "archive/wal_005/a*?[x]//../"
			}
			ctx := context.WithValue(context.Background(), prefixContextKey{}, "listing-context")
			calls := 0
			transport := prefixTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				require.Equal(t, "listing-context", r.Context().Value(prefixContextKey{}))
				require.LessOrEqual(t, calls, 2)
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "/bucket", r.URL.Path)
				q := r.URL.Query()
				require.Equal(t, fullPrefix, q.Get("prefix"))
				require.Empty(t, q.Get("delimiter"))
				require.False(t, q.Has("versions"))
				marker := "continuation-token"
				tokenParam := "continuation-token"
				require.Equal(t, "2", q.Get("list-type"))

				if calls == 1 {
					require.Empty(t, q.Get(tokenParam))
				} else {
					require.Equal(t, marker, q.Get(tokenParam))
				}
				status := http.StatusOK
				body := "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
				switch {
				case scenario == "first failure" || calls == 2 && scenario == "later failure":
					status = http.StatusForbidden
					body = "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>"
				case scenario == "first not found" || calls == 2 && scenario == "later not found":
					status = http.StatusNotFound
					body = "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>"
				case scenario != "empty":
					next := ""
					if calls == 1 {
						next = "<NextMarker>continuation-token</NextMarker><NextContinuationToken>continuation-token</NextContinuationToken>"
					}
					body = fmt.Sprintf(`<ListBucketResult><IsTruncated>%t</IsTruncated>%s<Contents><Key>%s%08d.lz4</Key><LastModified>2026-10-06T12:00:00Z</LastModified><Size>123</Size></Contents></ListBucketResult>`, calls == 1, next, fullPrefix, calls)
				}
				if scenario == "missing size" {
					body = strings.ReplaceAll(body, "<Size>123</Size>", "")
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/xml"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			client := awss3.New(awss3.Options{
				Region: "us-east-1", BaseEndpoint: aws.String("https://storage.example"),
				Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
				UsePathStyle: true, RetryMaxAttempts: 1, HTTPClient: &http.Client{Transport: transport},
			})
			folder := s3.NewFolder(client, nil, "archive/wal_005/", &s3.Config{Bucket: "bucket", EnableVersioning: "enabled"})
			lister, ok := interface{}(folder).(storage.PrefixLister)
			require.True(t, ok, "S3 must support native prefix listing")
			objects, err := lister.ListObjectsWithPrefix(ctx, prefix)
			if strings.Contains(scenario, "failure") {
				require.Error(t, err)
				require.Empty(t, objects)
			} else {
				require.NoError(t, err)
				if scenario == "empty" || scenario == "first not found" {
					require.Empty(t, objects)
				} else {
					wantObjects := 2
					if scenario == "later not found" {
						wantObjects = 1
					}
					require.Len(t, objects, wantObjects)
					for i, obj := range objects {
						require.Equal(t, fmt.Sprintf("%s%08d.lz4", prefix, i+1), obj.GetName())
						wantSize := int64(123)
						if scenario == "missing size" {
							wantSize = 0
						}
						require.Equal(t, wantSize, obj.GetSize())
						require.Equal(t, time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC), obj.GetLastModified())
					}
				}
			}
			wantCalls := 2
			if scenario == "empty" || scenario == "first failure" || scenario == "first not found" {
				wantCalls = 1
			}
			require.Equal(t, wantCalls, calls)
		})
	}
}
