package s3

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
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
