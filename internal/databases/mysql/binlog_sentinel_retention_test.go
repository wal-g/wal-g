package mysql_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/pkg/storages/s3"
)

func TestBinlogSentinelSkipsRetention(t *testing.T) {
	for _, mode := range []string{"GOVERNANCE", "COMPLIANCE"} {
		for _, operation := range []string{"upload", "copy"} {
			t.Run(mode+"/"+operation, func(t *testing.T) {
				var mu sync.Mutex
				puts := make(map[string]http.Header)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodHead {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					if r.Method != http.MethodPut {
						t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					mu.Lock()
					puts[r.URL.Path] = r.Header.Clone()
					mu.Unlock()
					w.WriteHeader(http.StatusOK)
				}))
				t.Cleanup(server.Close)
				st, err := s3.ConfigureStorage(t.Context(), "s3://bucket/prefix", map[string]string{
					"AWS_ENDPOINT":            server.URL,
					"AWS_REGION":              "us-east-1",
					"AWS_ACCESS_KEY_ID":       "test-key",
					"AWS_SECRET_ACCESS_KEY":   "test-secret",
					"AWS_S3_FORCE_PATH_STYLE": "true",
					"S3_SKIP_VALIDATION":      "true",
					"UPLOAD_CONCURRENCY":      "1",
					"S3_RETENTION_PERIOD":     "604800",
					"S3_RETENTION_MODE":       mode,
				})
				require.NoError(t, err)
				folder := st.RootFolder()
				if operation == "upload" {
					require.NoError(t, mysql.UploadBinlogSentinel(t.Context(), folder,
						&mysql.BinlogSentinelDto{GTIDArchived: "uuid:1-3"}))
				} else {
					plan, err := mysql.BuildCopyPlan(t.Context(), mysqlCopyFixture(t, true), folder,
						"base_incremental", true, "")
					require.NoError(t, err)
					require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))
				}
				// Reuse the destination to verify that the override cannot leak to other writes.
				require.NoError(t, folder.PutObject(t.Context(), "binlog/next", strings.NewReader("data")))
				mu.Lock()
				defer mu.Unlock()
				sentinelPath := "/bucket/prefix/" + mysql.BinlogSentinelPath
				require.Contains(t, puts, sentinelPath)
				require.Greater(t, len(puts), 1)
				for path, headers := range puts {
					if path == sentinelPath {
						require.Empty(t, headers.Get("X-Amz-Object-Lock-Mode"))
						require.Empty(t, headers.Get("X-Amz-Object-Lock-Retain-Until-Date"))
					} else {
						require.Equal(t, mode, headers.Get("X-Amz-Object-Lock-Mode"), path)
						require.NotEmpty(t, headers.Get("X-Amz-Object-Lock-Retain-Until-Date"), path)
					}
				}
			})
		}
	}
}
