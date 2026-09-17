package s3

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalgRetryablesConnReset(t *testing.T) {
	err := &net.OpError{
		Op:     "mock",
		Net:    "mock",
		Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
		Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
		Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
	}
	assert.Equal(t, aws.TrueTernary, walgRetryables{}.IsErrorRetryable(err))
}

func TestWalgRetryablesRandomError(t *testing.T) {
	err := fmt.Errorf("some strange unknown error")
	assert.Equal(t, aws.UnknownTernary, walgRetryables{}.IsErrorRetryable(err))
}

func TestWalgRetryablesNoError(t *testing.T) {
	assert.Equal(t, aws.UnknownTernary, walgRetryables{}.IsErrorRetryable(nil))
}

func TestWalgRetryablesOperationAborted(t *testing.T) {
	respErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 409}},
		Err:      fmt.Errorf("operation aborted"),
	}
	assert.Equal(t, aws.TrueTernary, walgRetryables{}.IsErrorRetryable(respErr))
}

func TestWalgRetryablesTooManyRequests(t *testing.T) {
	respErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 429}},
		Err:      fmt.Errorf("too many requests"),
	}
	assert.Equal(t, aws.TrueTernary, walgRetryables{}.IsErrorRetryable(respErr))
}

func TestWalgRetryablesClientDisconnected(t *testing.T) {
	respErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 499}},
		Err:      fmt.Errorf("client disconnected"),
	}
	assert.Equal(t, aws.TrueTernary, walgRetryables{}.IsErrorRetryable(respErr))
}

func TestUploadPartRetriesMalformedBadRequest(t *testing.T) {
	const htmlError = "<html>\n<head><title>400 Bad Request</title></head>\n" +
		"<body>\n<h1>Bad Request</h1>\n<hr>\n</body>\n</html>"
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		failures     int
		wantAttempts int
		wantError    bool
	}{
		{"HTML then success", 400, htmlError, 1, 2, false},
		{"truncated XML then success", 400, "<Error><Code>BadRequest", 1, 2, false},
		{"retry limit", 400, htmlError, 3, 3, true},
		{"valid BadRequest", 400, "<Error><Code>BadRequest</Code></Error>", 1, 1, true},
		{"valid AccessDenied", 403, "<Error><Code>AccessDenied</Code></Error>", 1, 1, true},
		{"malformed Forbidden", 403, htmlError, 1, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			client := awss3.NewFromConfig(aws.Config{
				Region:      "us-east-1",
				Credentials: credentials.NewStaticCredentialsProvider("key", "secret", ""),
				Retryer: newRetryerFunc(&Config{
					MaxRetries:              2,
					MaxThrottlingRetryDelay: time.Nanosecond,
				}),
				RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
				HTTPClient: smithyhttp.ClientDoFunc(func(req *http.Request) (*http.Response, error) {
					attempts++
					body, err := io.ReadAll(req.Body)
					require.NoError(t, err)
					require.NoError(t, req.Body.Close())
					assert.Equal(t, "part contents", string(body))
					assert.Equal(t, "upload-id", req.URL.Query().Get("uploadId"))
					assert.Equal(t, "1", req.URL.Query().Get("partNumber"))

					status, responseBody := http.StatusOK, ""
					if attempts <= tc.failures {
						status, responseBody = tc.status, tc.body
					}
					return &http.Response{
						StatusCode: status,
						Header:     http.Header{"Etag": []string{`"part-etag"`}},
						Body:       io.NopCloser(strings.NewReader(responseBody)),
						Request:    req,
					}, nil
				}),
			})
			output, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
				Bucket:     aws.String("bucket"),
				Key:        aws.String("backup.tar.br"),
				UploadId:   aws.String("upload-id"),
				PartNumber: aws.Int32(1),
				Body:       strings.NewReader("part contents"),
			})
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, `"part-etag"`, aws.ToString(output.ETag))
			}
			assert.Equal(t, tc.wantAttempts, attempts)
		})
	}
}
