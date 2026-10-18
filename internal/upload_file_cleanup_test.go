package internal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/none"
	"github.com/wal-g/wal-g/internal/ioextensions"
	walgS3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/time/rate"
)

type observedCompressor struct {
	compression.Compressor
	closed chan struct{}
}

func (c observedCompressor) NewWriter(w io.Writer) ioextensions.WriteFlushCloser {
	return observedCompressionWriter{WriteFlushCloser: c.Compressor.NewWriter(w), closed: c.closed}
}

type observedCompressionWriter struct {
	ioextensions.WriteFlushCloser
	closed chan struct{}
}

func (w observedCompressionWriter) Close() error {
	defer close(w.closed)
	return w.WriteFlushCloser.Close()
}

type earlyFailureFolder struct{ storage.Folder }

func (earlyFailureFolder) PutObject(context.Context, string, io.Reader) error {
	return errors.New("storage refused upload without consuming the body")
}

func waitCompressionClosed(t *testing.T, closed <-chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("compression producer leaked after upload returned")
	}
}

func TestUploadFileClosesCompressionOnEarlyError(t *testing.T) {
	for name, compressor := range compression.Compressors {
		t.Run(name, func(t *testing.T) {
			closed := make(chan struct{})
			u := internal.NewRegularUploader(observedCompressor{compressor, closed}, earlyFailureFolder{})
			err := u.UploadFile(t.Context(), ioextensions.NewNamedReaderImpl(bytes.NewReader(bytes.Repeat([]byte("WAL"), 32768)), "test"))
			require.Error(t, err)
			waitCompressionClosed(t, closed)
		})
	}
}

// Exercise the deployed SDK v1 uploader, including failures before it has read
// the whole WAL. No source-reader or goroutine-count heuristics are needed: each
// compression writer must actually finish closing.
func TestUploadFileS3FailureCleanupAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		partSize int
		limited  bool
	}{
		{"multipart", 5 << 20, false},
		{"default_part_size", 20 << 20, false},
		{"limiter_deadline", 20 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			var healthy atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if healthy.Load() {
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("ETag", `"test"`)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprint(w, "<Error><Code>SlowDown</Code><Message>test</Message></Error>")
			}))
			defer srv.Close()
			sess := session.Must(session.NewSession(&aws.Config{
				Endpoint: aws.String(srv.URL), Region: aws.String("test"), S3ForcePathStyle: aws.Bool(true),
				Credentials: credentials.NewStaticCredentials("test", "test", ""),
				Retryer: walgS3.NewConnResetRetryer(client.DefaultRetryer{NumMaxRetries: 2,
					MinThrottleDelay: time.Millisecond, MaxThrottleDelay: 2 * time.Millisecond}),
			}))
			api := awss3.New(sess)
			up := walgS3.NewUploader(walgS3.CreateUploaderAPI(api, tc.partSize, 1), "", "", "", "STANDARD", "", 0)
			var folder storage.Folder = walgS3.NewFolder(api, up, "", &walgS3.Config{Bucket: "test"})
			if tc.limited {
				folder = internal.NewLimitedFolder(folder, rate.NewLimiter(65536, 65536))
			}
			u := internal.NewRegularUploader(none.Compressor{}, folder)
			payload := bytes.Repeat([]byte("x"), 16<<20)
			for range 3 {
				closed := make(chan struct{})
				u.Compressor = observedCompressor{none.Compressor{}, closed}
				ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
				err := u.UploadFile(ctx, ioextensions.NewNamedReaderImpl(bytes.NewReader(payload), "test"))
				cancel()
				require.Error(t, err)
				waitCompressionClosed(t, closed)
			}
			if tc.limited {
				require.Zero(t, requests.Load(), "deadline must occur while filling the first part")
			} else {
				require.EqualValues(t, 9, requests.Load(), "three failed uploads with two retries each")
			}
			if !tc.limited && tc.partSize == 20<<20 {
				healthy.Store(true)
				closed := make(chan struct{})
				u.Compressor = observedCompressor{none.Compressor{}, closed}
				require.NoError(t, u.UploadFile(t.Context(), ioextensions.NewNamedReaderImpl(bytes.NewReader(payload), "test")))
				waitCompressionClosed(t, closed)
			}
		})
	}
}
