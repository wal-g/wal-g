package copy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/statistics"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	walgS3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestMultipartCopyRetries(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), int(manager.MinUploadPartSize)+37)
	for _, mode := range []string{"source EOF", "part 500", "exhausted SDK retries"} {
		for _, common := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/common=%t", mode, common), func(t *testing.T) {
				server := &multipartServer{mode: mode, parts: make(map[string][]byte)}
				client := awss3.NewFromConfig(aws.Config{Region: "us-east-1",
					Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
					RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
					Retryer: func() aws.Retryer {
						return retry.NewStandard(func(o *retry.StandardOptions) {
							o.MaxAttempts = 2
							o.MaxBackoff = time.Nanosecond
						})
					}, HTTPClient: smithyhttp.ClientDoFunc(server.do)})
				uploader := manager.NewUploader(client, func(u *manager.Uploader) {
					u.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
					u.PartSize = manager.MinUploadPartSize
					u.Concurrency = 1
				})
				destination := walgS3.NewFolder(client, walgS3.NewUploader(uploader, "", "", "", "STANDARD", "", -1), "",
					&walgS3.Config{Bucket: "bucket", MaxRetries: 1})
				to := &retryTestFolder{Folder: memory.NewFolder("target/", memory.NewKVS()),
					put: destination.PutObject, classify: destination.IsRetryableError}
				base := memory.NewFolder("source/", memory.NewKVS())
				require.NoError(t, base.PutObject(t.Context(), "part.tar.br", bytes.NewReader(payload)))
				reads, closes := 0, 0
				from := &retryTestFolder{Folder: base, read: func(context.Context, string) (io.ReadCloser, error) {
					reads++
					var r io.Reader = bytes.NewReader(payload)
					if mode == "source EOF" && reads == 1 {
						r = &brokenReader{data: payload[:manager.MinUploadPartSize+7], err: io.ErrUnexpectedEOF}
					}
					return &closeReader{r, func() error { closes++; return nil }}, nil
				}}
				retries := 1
				options := ExecuteOptions{MaxRetries: &retries}
				totalBefore, failedBefore := uploadedCounters(t)
				if common {
					objects, err := storage.ListFolderRecursively(t.Context(), from)
					require.NoError(t, err)
					infos := BuildCopyingInfos(from, to, objects, func(storage.Object) bool { return true }, NoopRenameFunc, nil)
					require.NoError(t, InfosWithOptions(t.Context(), infos, options))
				} else {
					plan, err := NewPlan(t.Context(), from, to)
					require.NoError(t, err)
					require.NoError(t, plan.AddObject("part.tar.br", "part.tar.br", PayloadPhase, false))
					require.NoError(t, Execute(t.Context(), plan, options))
				}
				totalAfter, failedAfter := uploadedCounters(t)
				require.Equal(t, totalBefore+1, totalAfter)
				require.Equal(t, failedBefore, failedAfter)
				require.True(t, bytes.Equal(payload, server.completed), "uploaded length %d, expected %d; prefix %q",
					len(server.completed), len(payload), server.completed[:min(64, len(server.completed))])
				require.Equal(t, reads, closes)
				require.Equal(t, 1, server.completes)
				if mode == "part 500" {
					require.Equal(t, 1, reads)
					require.Equal(t, 1, server.starts)
					require.Zero(t, server.aborts)
					require.Equal(t, 1, server.failures)
				} else {
					require.Equal(t, 2, reads)
					require.Equal(t, 2, server.starts)
					require.Equal(t, 1, server.aborts)
					if mode == "exhausted SDK retries" {
						require.Equal(t, 2, server.failures)
					}
				}
			})
		}
	}
}

func uploadedCounters(t *testing.T) (float64, float64) {
	t.Helper()
	total, failed := &dto.Metric{}, &dto.Metric{}
	require.NoError(t, statistics.WalgMetrics.UploadedFilesTotal.Write(total))
	require.NoError(t, statistics.WalgMetrics.UploadedFilesFailedTotal.Write(failed))
	return total.GetCounter().GetValue(), failed.GetCounter().GetValue()
}

type multipartServer struct {
	mu                                  sync.Mutex
	mode                                string
	starts, aborts, completes, failures int
	parts                               map[string][]byte
	completed                           []byte
}

func (s *multipartServer) do(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var data []byte
	if req.Body != nil {
		var err error
		data, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
	}
	query := req.URL.Query()
	status, body := http.StatusOK, ""
	switch {
	case req.Method == http.MethodPost && query.Has("uploads"):
		s.starts++
		body = fmt.Sprintf("<InitiateMultipartUploadResult><UploadId>upload-%d</UploadId></InitiateMultipartUploadResult>", s.starts)
	case req.Method == http.MethodDelete:
		s.aborts++
		status = http.StatusNoContent
	case req.Method == http.MethodPut && query.Has("partNumber"):
		if (s.mode == "part 500" && s.failures == 0) || (s.mode == "exhausted SDK retries" && query.Get("uploadId") == "upload-1") {
			s.failures++
			status = 500
			body = "<Error><Code>InternalError</Code></Error>"
		} else {
			s.parts[query.Get("uploadId")+"/"+query.Get("partNumber")] = data
		}
	case req.Method == http.MethodPost && query.Has("uploadId"):
		s.completes++
		s.completed = append(s.completed, s.parts[query.Get("uploadId")+"/1"]...)
		s.completed = append(s.completed, s.parts[query.Get("uploadId")+"/2"]...)
		body = "<CompleteMultipartUploadResult><ETag>complete</ETag></CompleteMultipartUploadResult>"
	default:
		return nil, fmt.Errorf("unexpected test request %s %s", req.Method, req.URL.Path)
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Etag": []string{`"part"`}},
		Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}
