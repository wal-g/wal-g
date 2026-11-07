package internal_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	cloudgcs "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	cloudoss "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscred "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	cloudswift "github.com/ncw/swift/v2"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/azure"
	"github.com/wal-g/wal-g/pkg/storages/gcs"
	"github.com/wal-g/wal-g/pkg/storages/oss"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/swift"
	"google.golang.org/api/option"
)

type prefixObjectTransport struct {
	mu          sync.Mutex
	backend     string
	keys        map[string]int64
	deletes     []string
	failList    bool
	keepDeletes bool
}

func (f *prefixObjectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	status, body, contentType := 200, "", "application/xml"
	if r.Method == http.MethodDelete {
		key := strings.TrimPrefix(r.URL.Path, "/test/")
		if f.backend == "gcs" {
			key = strings.SplitN(r.URL.Path, "/o/", 2)[1]
		}
		f.deletes = append(f.deletes, key)
		if !f.keepDeletes {
			delete(f.keys, key)
		}
		status = 204
		if f.backend == "azure" {
			status = 202
		}
	} else if r.Method == http.MethodPost && f.backend == "oss" {
		var req struct {
			Objects []struct{ Key string } `xml:"Object"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
		for _, o := range req.Objects {
			f.deletes = append(f.deletes, o.Key)
			if !f.keepDeletes {
				delete(f.keys, o.Key)
			}
		}
		body = "<DeleteResult/>"
	} else if r.Method == http.MethodGet && r.URL.Query().Has("prefix") {
		if f.failList {
			return nil, fmt.Errorf("injected LIST failure")
		}
		q := r.URL.Query()
		marker := q.Get("marker")
		if f.backend == "gcs" {
			marker = q.Get("pageToken")
		}
		if f.backend == "oss" {
			marker = q.Get("continuation-token")
		}
		var keys []string
		for k := range f.keys {
			if strings.HasPrefix(k, q.Get("prefix")) && k > marker {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		next := ""
		if len(keys) > 1 {
			next = keys[0]
			keys = keys[:1]
		}
		switch f.backend {
		case "gcs":
			var items []map[string]any
			for _, k := range keys {
				items = append(items, map[string]any{"name": k, "size": fmt.Sprint(f.keys[k]), "updated": "2026-01-01T00:00:00Z"})
			}
			raw, _ := json.Marshal(map[string]any{"items": items, "nextPageToken": next})
			body = string(raw)
			contentType = "application/json"
		case "azure":
			body = `<EnumerationResults><Blobs>`
			for _, k := range keys {
				body += fmt.Sprintf(`<Blob><Name>%s</Name><Properties><Last-Modified>Thu, 01 Jan 2026 00:00:00 GMT</Last-Modified><Content-Length>%d</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>`, k, f.keys[k])
			}
			body += "</Blobs><NextMarker>" + next + "</NextMarker></EnumerationResults>"
		case "oss":
			body = fmt.Sprintf(`<ListBucketResult><IsTruncated>%t</IsTruncated><NextContinuationToken>%s</NextContinuationToken>`, next != "", next)
			for _, k := range keys {
				body += fmt.Sprintf(`<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents>`, k, f.keys[k])
			}
			body += "</ListBucketResult>"
		case "swift":
			items := []map[string]any{}
			for _, k := range keys {
				items = append(items, map[string]any{"name": k, "bytes": f.keys[k], "last_modified": "2026-01-01T00:00:00.000000", "content_type": "application/octet-stream", "hash": "hash"})
			}
			raw, _ := json.Marshal(items)
			body = string(raw)
			contentType = "application/json"
		}
	} else {
		return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL)
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}
func prefixReviewFolder(t *testing.T, backend, prefix string, normalize bool, tr *prefixObjectTransport) storage.Folder {
	t.Helper()
	client := &http.Client{Transport: tr}
	switch backend {
	case "gcs":
		c, err := cloudgcs.NewClient(t.Context(), option.WithEndpoint("https://fixture.invalid/storage/v1/"), option.WithHTTPClient(client), option.WithoutAuthentication())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		return gcs.NewFolder(c.Bucket("test"), strings.TrimSuffix(prefix, "/"), nil, &gcs.Config{NormalizePrefix: normalize, ContextTimeout: time.Minute})
	case "azure":
		c, err := container.NewClientWithNoCredential("https://fixture.invalid/test", &container.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: client}})
		require.NoError(t, err)
		return azure.NewFolder(prefix, c, azblob.UploadStreamOptions{}, time.Minute)
	case "oss":
		c := cloudoss.NewClient(cloudoss.LoadDefaultConfig().WithRegion("test").WithEndpoint("https://fixture.invalid").WithUsePathStyle(true).WithCredentialsProvider(osscred.NewStaticCredentialsProvider("test", "test")).WithHttpClient(client).WithRetryMaxAttempts(1))
		f := oss.NewFolder(c, "test", prefix, &oss.Config{UploadPartSize: 5 * 1024 * 1024})
		f.SetVersioningEnabled(t.Context(), false)
		return f
	case "swift":
		c := &cloudswift.Connection{StorageUrl: "https://fixture.invalid", AuthToken: "test", Transport: tr, FetchUntilEmptyPage: true}
		return swift.NewFolder(c, cloudswift.Container{Name: "test"}, prefix)
	}
	panic(backend)
}
func TestRecoveryWalkDeletesExactPrefixObjects(t *testing.T) {
	for _, backend := range []string{"gcs", "azure", "oss", "swift"} {
		for _, size := range []int64{0, 17} {
			for _, normalize := range []bool{false, true} {
				if backend != "gcs" && !normalize {
					continue
				}
				t.Run(fmt.Sprintf("%s/%d/%t", backend, size, normalize), func(t *testing.T) {
					prefix := "basebackups_005/B1/"
					if backend == "gcs" && !normalize {
						prefix = "archive//" + prefix
					}
					tr := &prefixObjectTransport{backend: backend, keys: map[string]int64{prefix: size, prefix + "nested/": 5, prefix + "payload": 7, strings.TrimSuffix(prefix, "/"): 9}}
					neighbor := strings.TrimSuffix(prefix, "B1/") + "B10/payload"
					tr.keys[neighbor] = 4
					folder := prefixReviewFolder(t, backend, prefix, normalize, tr)
					var objects []storage.Object
					require.NoError(t, storage.WalkObjects(t.Context(), folder, func(o storage.Object) error { objects = append(objects, o); return nil }))
					require.Len(t, objects, 3)
					require.Equal(t, "", objects[0].GetName())
					require.Equal(t, size, objects[0].GetSize())
					plan := &internal.DeletionPlan{Root: folder}
					defer plan.Close()
					require.NoError(t, plan.PlanObjects(t.Context(), "payload", folder, func(storage.Object) bool { return true }))
					require.NoError(t, plan.Execute(t.Context(), false))
					require.Empty(t, tr.deletes)
					require.NoError(t, plan.Execute(t.Context(), true))
					require.ElementsMatch(t, []string{prefix, prefix + "nested/", prefix + "payload"}, tr.deletes)
					require.NoError(t, storage.WalkObjects(t.Context(), folder, func(storage.Object) error { return fmt.Errorf("leftover object") }))
					require.Equal(t, map[string]int64{strings.TrimSuffix(prefix, "/"): 9, neighbor: 4}, tr.keys)
					tr.keys[prefix] = size
					visitorErr := fmt.Errorf("stop visitor")
					require.ErrorIs(t, storage.WalkObjects(t.Context(), folder, func(storage.Object) error { return visitorErr }), visitorErr)
					tr.failList = true
					ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
					defer cancel()
					require.Error(t, storage.WalkObjects(ctx, folder, func(storage.Object) error { return nil }))
				})
			}
		}
	}
}
