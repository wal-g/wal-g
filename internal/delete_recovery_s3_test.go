package internal_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/databases/redis"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/time/rate"
)

func TestRecoveryS3GET500StillAllowsDeletion(t *testing.T) {
	for db, build := range map[string]internal.RecoveryPlanner{"redis": redis.PlanRecoveryDeletion, "mongo": mongo.PlanRecoveryDeletion,
		"mysql": mysql.PlanRecoveryDeletion, "postgres": postgres.PlanRecoveryDeletion, "greenplum": greenplum.PlanRecoveryDeletion} {
		t.Run(db, func(t *testing.T) {
			var mu sync.Mutex
			keys := map[string]bool{utility.BaseBackupPath + "B1" + utility.SentinelSuffix: true, utility.BaseBackupPath + "B1/metadata.json": true,
				utility.BaseBackupPath + "B1/payload": true, utility.BaseBackupPath + "B1/": true, utility.BaseBackupPath + "journal_B1": true, utility.BaseBackupPath + "unrelated/payload": true}
			reads, deletes := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/xml")
				if r.URL.Query().Get("list-type") == "2" {
					prefix, delimiter := r.URL.Query().Get("prefix"), r.URL.Query().Get("delimiter")
					type entry struct {
						Key          string
						LastModified string
						Size         int64
					}
					type common struct{ Prefix string }
					result := struct {
						XMLName        xml.Name `xml:"ListBucketResult"`
						IsTruncated    bool
						Contents       []entry
						CommonPrefixes []common
					}{}
					seen := map[string]bool{}
					for key := range keys {
						if !strings.HasPrefix(key, prefix) {
							continue
						}
						suffix := strings.TrimPrefix(key, prefix)
						if delimiter != "" && strings.Contains(suffix, delimiter) {
							p := prefix + strings.SplitN(suffix, delimiter, 2)[0] + delimiter
							if !seen[p] {
								result.CommonPrefixes = append(result.CommonPrefixes, common{p})
								seen[p] = true
							}
							continue
						}
						result.Contents = append(result.Contents, entry{key, "2026-01-01T00:00:00Z", 1})
					}
					_ = xml.NewEncoder(w).Encode(result)
					return
				}
				if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
					var request struct {
						Objects []struct{ Key string } `xml:"Object"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
						http.Error(w, err.Error(), 400)
						return
					}
					for _, object := range request.Objects {
						delete(keys, object.Key)
						deletes++
					}
					fmt.Fprint(w, "<DeleteResult/>")
					return
				}
				reads++
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, "<Error><Code>InternalError</Code><Message>Object cannot be read</Message></Error>")
			}))
			defer server.Close()
			client := awss3.NewFromConfig(aws.Config{Region: "test", Credentials: aws.AnonymousCredentials{}, HTTPClient: server.Client()}, func(o *awss3.Options) {
				o.BaseEndpoint = aws.String(server.URL)
				o.UsePathStyle = true
				o.RetryMaxAttempts = 1
			})
			root := walgs3.NewFolder(client, nil, "", &walgs3.Config{Bucket: "test", EnableVersioning: "disabled", DeleteBatchSize: 500})
			plan, err := build(t.Context(), internal.NewLimitedFolder(root, rate.NewLimiter(rate.Inf, 1)), "default", []string{"B1"}, false)
			require.NoError(t, err)
			defer plan.Close()
			_ = plan.Execute(t.Context(), false)
			require.Zero(t, deletes)
			err = plan.Execute(t.Context(), true)
			if db == "greenplum" {
				require.ErrorContains(t, err, "mapping")
			} else {
				require.NoError(t, err)
			}
			require.Positive(t, reads)
			expected := map[string]bool{utility.BaseBackupPath + "unrelated/payload": true}
			require.Equal(t, expected, keys)
		})
	}
}
