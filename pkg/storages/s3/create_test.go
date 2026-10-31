package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// Exercise the real SDK serialization and response handling without a live S3.
type conditionalPutClient struct {
	mu            sync.Mutex
	objects       map[string][]byte
	headers       []http.Header
	status        int
	err           error
	errAfterWrite error
}

func (c *conditionalPutClient) Do(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	c.headers = append(c.headers, req.Header.Clone())
	status, body := http.StatusOK, ""
	if req.Method != http.MethodPut || req.Header.Get("If-None-Match") != "*" {
		status = http.StatusBadRequest
	} else if _, exists := c.objects[req.URL.Path]; exists {
		status, body = http.StatusPreconditionFailed, "<Error><Code>PreconditionFailed</Code></Error>"
	} else if c.status != 0 {
		status, body = c.status, "<Error><Code>AccessDenied</Code></Error>"
	} else {
		data, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		c.objects[req.URL.Path] = data
		if c.errAfterWrite != nil {
			return nil, c.errAfterWrite
		}
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}

func reservationS3Folder(client *conditionalPutClient) *Folder {
	cfg := aws.Config{Region: "us-east-1", HTTPClient: client,
		Credentials: credentials.NewStaticCredentialsProvider("key", "secret", ""), RetryMaxAttempts: 1}
	svc := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String("https://s3.example.com")
		o.UsePathStyle = true
	})
	uploader := NewUploader(nil, "aws:kms", "", "kms-key", "STANDARD", "GOVERNANCE", 60)
	return NewFolder(svc, uploader, "cluster/", &Config{Bucket: "bucket"})
}

func TestCreateObjectConditionalPut(t *testing.T) {
	client := &conditionalPutClient{objects: make(map[string][]byte)}
	results := make(chan error, 16)
	var wg sync.WaitGroup
	for range cap(results) {
		wg.Go(func() {
			results <- reservationS3Folder(client).CreateObject(t.Context(), "reservation", []byte("owner"))
		})
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, storage.ErrObjectAlreadyExists)
		}
	}
	require.Equal(t, 1, winners)
	require.Equal(t, []byte("owner"), client.objects["/bucket/cluster/reservation"])
	for _, header := range client.headers {
		require.Equal(t, "*", header.Get("If-None-Match"))
		require.Equal(t, "aws:kms", header.Get("X-Amz-Server-Side-Encryption"))
		require.Equal(t, "kms-key", header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"))
		require.Equal(t, "GOVERNANCE", header.Get("X-Amz-Object-Lock-Mode"))
	}
}

func TestCreateObjectFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		err    error
	}{
		{"denied", http.StatusForbidden, nil},
		{"conflict", http.StatusConflict, nil},
		{"connection lost", 0, errors.New("response lost")},
		{"timeout", 0, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &conditionalPutClient{objects: make(map[string][]byte), status: tc.status, err: tc.err}
			err := reservationS3Folder(client).CreateObject(t.Context(), "reservation", nil)
			require.Error(t, err)
			require.NotErrorIs(t, err, storage.ErrObjectAlreadyExists)
			require.Empty(t, client.objects)
		})
	}
}

func TestCreateObjectLostResponseRetainsReservation(t *testing.T) {
	client := &conditionalPutClient{objects: make(map[string][]byte), errAfterWrite: errors.New("response lost after commit")}
	folder := reservationS3Folder(client)
	require.Error(t, folder.CreateObject(t.Context(), "reservation", []byte("owner")))
	require.Equal(t, []byte("owner"), client.objects["/bucket/cluster/reservation"])
	client.errAfterWrite = nil
	require.ErrorIs(t, folder.CreateObject(t.Context(), "reservation", []byte("retry")), storage.ErrObjectAlreadyExists)
	require.Equal(t, []byte("owner"), client.objects["/bucket/cluster/reservation"])
}
