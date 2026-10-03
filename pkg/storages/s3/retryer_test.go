package s3

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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
	// Logged Backblaze failure: HTTP 400, InvalidRequest, "The request body was too small"
	// (request id 26e2e482f367dfc6). MinIO reports the same mismatch as IncompleteBody.
	const partBody = "part contents"
	shortBody := s3ErrorXML("InvalidRequest", "The request body was too small")
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		failures     int
		wantAttempts int
		wantError    bool
		cancel       bool
	}{
		{"HTML then success", 400, htmlError, 1, 2, false, false},
		{"truncated XML then success", 400, "<Error><Code>BadRequest", 1, 2, false, false},
		{"retry limit", 400, htmlError, 3, 3, true, false},
		{"valid BadRequest", 400, "<Error><Code>BadRequest</Code></Error>", 1, 1, true, false},
		{"valid AccessDenied", 403, "<Error><Code>AccessDenied</Code></Error>", 1, 1, true, false},
		{"malformed Forbidden", 403, htmlError, 1, 1, true, false},
		{"backblaze short body then success", 400, shortBody, 1, 2, false, false},
		{
			"short body normalized case and punctuation",
			400,
			s3ErrorXML("InvalidRequest", "  THE REQUEST BODY WAS TOO SMALL...  "),
			1, 2, false, false,
		},
		{
			"unrelated InvalidRequest",
			400,
			s3ErrorXML("InvalidRequest", "The specified upload does not exist."),
			1, 1, true, false,
		},
		{
			"phrase on a different error code",
			400,
			s3ErrorXML("BadRequest", "The request body was too small"),
			1, 1, true, false,
		},
		{
			"non-400 short body message",
			403,
			s3ErrorXML("InvalidRequest", "The request body was too small"),
			1, 1, true, false,
		},
		{
			"minio incomplete body then success",
			400,
			s3ErrorXML("IncompleteBody",
				"You did not provide the number of bytes specified by the Content-Length HTTP header."),
			1, 2, false, false,
		},
		{"short body retry limit", 400, shortBody, 3, 3, true, false},
		{"cancelled during short body", 400, shortBody, 3, 1, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
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
					assert.Equal(t, partBody, string(body))
					assert.Equal(t, "upload-id", req.URL.Query().Get("uploadId"))
					assert.Equal(t, "1", req.URL.Query().Get("partNumber"))
					// Seekable part bodies get a content length. Retries must
					// resend that same length with the same bytes.
					if req.ContentLength >= 0 {
						assert.Equal(t, int64(len(partBody)), req.ContentLength)
					} else {
						assert.Equal(t, strconv.Itoa(len(partBody)), req.Header.Get("Content-Length"))
					}
					if headerLen := req.Header.Get("Content-Length"); headerLen != "" {
						assert.Equal(t, strconv.Itoa(len(partBody)), headerLen)
					}

					status, responseBody := http.StatusOK, ""
					if attempts <= tc.failures {
						if tc.cancel {
							cancel()
						}
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
			output, err := client.UploadPart(ctx, &awss3.UploadPartInput{
				Bucket:     aws.String("bucket"),
				Key:        aws.String("backup.tar.br"),
				UploadId:   aws.String("upload-id"),
				PartNumber: aws.Int32(1),
				Body:       strings.NewReader(partBody),
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

func TestWalgRetryablesShortRequestBody(t *testing.T) {
	exact := &smithy.GenericAPIError{
		Code:    "InvalidRequest",
		Message: "The request body was too small",
	}
	for _, tc := range []struct {
		name string
		err  error
		want aws.Ternary
	}{
		{"exact message", s3ResponseError(400, exact), aws.TrueTernary},
		{
			"normalized case and terminal punctuation",
			s3ResponseError(400, &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "  the request body was too small.  ",
			}),
			aws.TrueTernary,
		},
		{
			"upper case and exclamation",
			s3ResponseError(400, &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "THE REQUEST BODY WAS TOO SMALL!",
			}),
			aws.TrueTernary,
		},
		{
			"ellipsis",
			s3ResponseError(400, &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "The request body was too small…",
			}),
			aws.TrueTernary,
		},
		{
			"typed InvalidRequest",
			s3ResponseError(400, &types.InvalidRequest{
				Message: aws.String("The request body was too small."),
			}),
			aws.TrueTernary,
		},
		{
			"sdk response error wrapper",
			&awshttp.ResponseError{
				ResponseError: s3ResponseError(400, exact),
				RequestID:     "26e2e482f367dfc6",
			},
			aws.TrueTernary,
		},
		{
			"wrapped operation error",
			&smithy.OperationError{
				ServiceID:     "S3",
				OperationName: "UploadPart",
				Err:           s3ResponseError(400, exact),
			},
			aws.TrueTernary,
		},
		{
			"fmt wrapped",
			fmt.Errorf("upload multipart failed: %w", s3ResponseError(400, exact)),
			aws.TrueTernary,
		},
		{
			"unrelated InvalidRequest",
			s3ResponseError(400, &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "The specified upload does not exist.",
			}),
			aws.UnknownTernary,
		},
		{
			"phrase contained in a longer InvalidRequest",
			s3ResponseError(400, &smithy.GenericAPIError{
				Code:    "InvalidRequest",
				Message: "The request body was too small for the requested part",
			}),
			aws.UnknownTernary,
		},
		{
			"plain error containing the phrase",
			fmt.Errorf("InvalidRequest: The request body was too small"),
			aws.UnknownTernary,
		},
		{
			"non-400 with the same message",
			s3ResponseError(500, exact),
			aws.UnknownTernary,
		},
		{
			"different code with the same message",
			s3ResponseError(400, &smithy.GenericAPIError{
				Code:    "BadRequest",
				Message: "The request body was too small",
			}),
			aws.UnknownTernary,
		},
		{
			"nil InvalidRequest message",
			s3ResponseError(400, &types.InvalidRequest{}),
			aws.UnknownTernary,
		},
		{
			"minio IncompleteBody",
			s3ResponseError(400, &smithy.GenericAPIError{
				Code:    "IncompleteBody",
				Message: "You did not provide the number of bytes specified by the Content-Length HTTP header.",
			}),
			aws.TrueTernary,
		},
		{
			"IncompleteBody without a message",
			s3ResponseError(400, &smithy.GenericAPIError{Code: "IncompleteBody"}),
			aws.TrueTernary,
		},
		{
			"IncompleteBody on a non-400 response",
			s3ResponseError(403, &smithy.GenericAPIError{Code: "IncompleteBody"}),
			aws.UnknownTernary,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, walgRetryables{}.IsErrorRetryable(tc.err))
		})
	}
}

func TestNewRetryerRetriesShortRequestBody(t *testing.T) {
	incomplete := s3ResponseError(400, &smithy.GenericAPIError{
		Code:    "IncompleteBody",
		Message: "You did not provide the number of bytes specified by the Content-Length HTTP header.",
	})
	// DefaultRetryableErrorCodes does not include IncompleteBody. The custom
	// rule is added only because the standard retryer leaves it unclassified.
	assert.False(t, retry.NewStandard().IsErrorRetryable(incomplete))

	shortBody := s3ResponseError(400, &smithy.GenericAPIError{
		Code:    "InvalidRequest",
		Message: "The request body was too small",
	})
	assert.False(t, retry.NewStandard().IsErrorRetryable(shortBody))

	retryer := newRetryerFunc(&Config{
		MaxRetries:              2,
		MaxThrottlingRetryDelay: time.Nanosecond,
	})()
	assert.True(t, retryer.IsErrorRetryable(incomplete))
	assert.True(t, retryer.IsErrorRetryable(shortBody))
	assert.Equal(t, 3, retryer.MaxAttempts())
}

// s3ErrorXML is the S3 XML shape logged for the Backblaze InvalidRequest,
// with the code and message substituted.
func s3ErrorXML(code, message string) string {
	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<Error><Code>%s</Code><Message>%s</Message>`+
			`<RequestId>26e2e482f367dfc6</RequestId>`+
			`<HostId>aZbw4SjRfZPkz3zHVNCo3PDcZY/Jjajgb</HostId></Error>`,
		code, message,
	)
}

func s3ResponseError(status int, err error) *smithyhttp.ResponseError {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      err,
	}
}
