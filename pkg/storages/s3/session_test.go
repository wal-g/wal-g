package s3

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestLoadAWSConfigWithHTTPSEndpointSourceTwice(t *testing.T) {
	config := &Config{
		Secrets:          &Secrets{},
		Region:           "us-east-1",
		Endpoint:         "https://s3.example.com",
		EndpointSource:   "https://endpoint-source.example.com",
		EndpointPort:     "443",
		EndpointProtocol: "https",
	}

	first, _, err := loadAWSConfig(t.Context(), config)
	require.NoError(t, err)
	second, _, err := loadAWSConfig(t.Context(), config)
	require.NoError(t, err)

	require.NotSame(t, first.HTTPClient, second.HTTPClient)
	assertSessionTransport(t, first.HTTPClient, "s3.example.com")
	assertSessionTransport(t, second.HTTPClient, "s3.example.com")
}

func TestLoadAWSConfigWithRequestTimeout(t *testing.T) {
	config := &Config{
		Secrets:        &Secrets{},
		Region:         "us-east-1",
		Endpoint:       "https://s3.example.com",
		RequestTimeout: 5 * time.Second,
	}

	awsConfig, _, err := loadAWSConfig(t.Context(), config)
	require.NoError(t, err)

	client, ok := awsConfig.HTTPClient.(*http.Client)
	require.True(t, ok)

	logging, ok := client.Transport.(*loggingTransport)
	require.True(t, ok)

	transport, ok := logging.underlying.(*http.Transport)
	require.True(t, ok)

	require.Equal(t, 5*time.Second, transport.ResponseHeaderTimeout)
}

func assertSessionTransport(t *testing.T, httpClient any, serverName string) {
	t.Helper()

	client, ok := httpClient.(*http.Client)
	require.True(t, ok)
	logging, ok := client.Transport.(*loggingTransport)
	require.True(t, ok)
	transport, ok := logging.underlying.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, transport.TLSClientConfig)
	require.Equal(t, serverName, transport.TLSClientConfig.ServerName)
}

// captureClient records the last request the SDK produced and replies with an empty 200.
type captureClient struct {
	header http.Header
	body   []byte
}

func (c *captureClient) Do(req *http.Request) (*http.Response, error) {
	c.header = req.Header.Clone()
	if req.Body != nil {
		c.body, _ = io.ReadAll(req.Body)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("<DeleteResult></DeleteResult>")),
		Request:    req,
	}, nil
}

func newCapturingClient(capture *captureClient) *s3.Client {
	cfg := aws.Config{
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider("key", "secret", ""),
		HTTPClient:                 capture,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported,
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://s3.example.com")
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
	})
}

// S3 SDK v2 sends x-amz-checksum-crc32 rather than Content-Md5, which MinIO rejects.
func TestContentMD5OnDeleteObjects(t *testing.T) {
	capture := &captureClient{}
	_, err := newCapturingClient(capture).DeleteObjects(t.Context(), &s3.DeleteObjectsInput{
		Bucket: aws.String("bucket"),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("object")}}},
	}, withContentMD5)
	require.NoError(t, err)

	sum := md5.Sum(capture.body)
	require.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), capture.header.Get("Content-Md5"))
	require.Empty(t, capture.header.Get("X-Amz-Checksum-Crc32"))
	require.Empty(t, capture.header.Get("X-Amz-Sdk-Checksum-Algorithm"))
}

// Only DeleteObjects opts in, uploads keep the SDK default checksum.
func TestChecksumKeptOnPutObject(t *testing.T) {
	capture := &captureClient{}
	_, err := newCapturingClient(capture).PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("object"),
		Body:   strings.NewReader("payload"),
	})
	require.NoError(t, err)

	require.Empty(t, capture.header.Get("Content-Md5"))
	require.NotEmpty(t, capture.header.Get("X-Amz-Checksum-Crc32"))
}

func TestEndpointSourceIsolationPreservesConfigHash(t *testing.T) {
	cfg := &Config{EndpointSource: "http://discovery.invalid"}
	before, err := storage.ComputeConfigHash("s3", cfg)
	require.NoError(t, err)
	cfg.IsolateEndpointSource = true
	after, err := storage.ComputeConfigHash("s3", cfg)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestEndpointSourceTransportIsolation(t *testing.T) {
	for _, isolate := range []bool{false, true} {
		t.Run(fmt.Sprintf("isolate=%t", isolate), func(t *testing.T) {
			original := http.DefaultTransport
			transport := original.(*http.Transport).Clone()
			transport.DisableKeepAlives = false
			http.DefaultTransport = transport
			t.Cleanup(func() { http.DefaultTransport = original; transport.CloseIdleConnections() })
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !r.Close {
					t.Error("discovery must close connections, including after redirects")
				}
				if r.URL.Path == "/redirect" {
					http.Redirect(w, r, "/node", http.StatusFound)
					return
				}
				_, _ = io.WriteString(w, "127.0.0.1")
			}))
			defer source.Close()

			var client *http.Client
			if isolate {
				client = newEndpointSourceClient()
				defer client.CloseIdleConnections()
			}
			got := requestEndpointFromSource(t.Context(), source.URL+"/redirect", "443", client)
			require.Equal(t, "127.0.0.1:443", aws.ToString(got))
			require.Equal(t, !isolate, transport.DisableKeepAlives,
				"primary behavior must stay unchanged; secondary discovery must not mutate the default transport")
		})
	}
}
