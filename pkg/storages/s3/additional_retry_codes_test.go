package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retryCodesTransport func(*http.Request) (*http.Response, error)

func (f retryCodesTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// Exercise the actual SDK v1 retry loop, including rewinding the body, through
// the storage configuration path. No live endpoint or credentials are used.
func TestS3AdditionalRetryCodes(t *testing.T) {
	for _, operation := range []string{"PutObject", "UploadPart"} {
		for _, tc := range []struct {
			name      string
			codes     string
			status    int
			retries   int
			alwaysErr bool
			cancel    bool
			attempts  int
			wantError bool
		}{
			{"default", "", 400, 2, false, false, 1, true},
			{"configured", "InvalidRequest", 400, 2, false, false, 2, false},
			{"spaces and empty items", " BadDigest , InvalidRequest, ,InvalidRequest ", 400, 2, false, false, 2, false},
			{"other code", "BadDigest", 400, 2, false, false, 1, true},
			{"case sensitive", "invalidrequest", 400, 2, false, false, 1, true},
			{"other HTTP status", "InvalidRequest", 403, 2, false, false, 2, false},
			{"exhausted", "InvalidRequest", 400, 2, true, false, 3, true},
			{"zero retries", "InvalidRequest", 400, 0, false, false, 1, true},
			{"cancelled", "InvalidRequest", 400, 2, false, true, 1, true},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				settings := map[string]string{
					endpointSetting: "https://retry.invalid", regionSetting: "us-east-1",
					accessKeyIDSetting: "key", secretAccessKeySetting: "secret",
					forcePathStyleSetting: "true", skipValidationSetting: "true",
					uploadConcurrencySetting: "1",
					maxRetriesSetting:        strconv.Itoa(tc.retries), retryableErrorCodesSetting: tc.codes,
				}
				st, err := ConfigureStorageWithFailover(t.Context(), "s3://bucket/prefix", settings)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, st.Close()) })
				s3client := st.RootFolder().(*Folder).s3API.(*awss3.S3)
				s3client.Config.HTTPClient.CloseIdleConnections()
				retryer := s3client.Config.Retryer.(*ConnResetRetryer)
				base := retryer.Retryer.(client.DefaultRetryer)
				base.MinRetryDelay, base.MaxRetryDelay = time.Millisecond, 10*time.Millisecond
				retryer.Retryer = base
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				attempts := 0
				const payload = "contents must be resent on every attempt"
				s3client.Config.HTTPClient = &http.Client{Transport: retryCodesTransport(func(req *http.Request) (*http.Response, error) {
					attempts++
					body, err := io.ReadAll(req.Body)
					require.NoError(t, err)
					require.NoError(t, req.Body.Close())
					assert.Equal(t, payload, string(body))
					status, responseBody := http.StatusOK, ""
					if attempts == 1 || tc.alwaysErr {
						status = tc.status
						responseBody = `<Error><Code>InvalidRequest</Code><Message>Invalid Request</Message></Error>`
					}
					if tc.cancel {
						cancel()
					}
					return &http.Response{
						StatusCode: status, Header: http.Header{"Etag": []string{`"etag"`}},
						Body: io.NopCloser(strings.NewReader(responseBody)), Request: req,
					}, nil
				})}
				if operation == "PutObject" {
					_, err = s3client.PutObjectWithContext(ctx, &awss3.PutObjectInput{
						Bucket: aws.String("bucket"), Key: aws.String("wal.lz4"), Body: strings.NewReader(payload),
					})
				} else {
					_, err = s3client.UploadPartWithContext(ctx, &awss3.UploadPartInput{
						Bucket: aws.String("bucket"), Key: aws.String("backup.tar.br"),
						UploadId: aws.String("upload-id"), PartNumber: aws.Int64(1), Body: strings.NewReader(payload),
					})
				}
				if tc.wantError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				assert.Equal(t, tc.attempts, attempts)
			})
		}
	}
}

func TestAdditionalRetryCodesDoNotReplaceDefaults(t *testing.T) {
	retryer := NewConnResetRetryer(client.DefaultRetryer{NumMaxRetries: 2})
	retryer.additionalErrorCodes = parseErrorCodes("InvalidRequest")
	for _, status := range []int{409, 429, 500, 503} {
		assert.True(t, retryer.ShouldRetry(&request.Request{HTTPResponse: &http.Response{StatusCode: status}}))
	}
	assert.True(t, retryer.ShouldRetry(&request.Request{Error: fmt.Errorf("transport: connection reset by peer")}))
	assert.True(t, retryer.ShouldRetry(&request.Request{
		Error: fmt.Errorf("wrapped: %w", awserr.New("InvalidRequest", "invalid", nil)),
	}))
	assert.False(t, retryer.ShouldRetry(&request.Request{Error: awserr.New("AccessDenied", "denied", nil)}))
	assert.False(t, NewConnResetRetryer(client.DefaultRetryer{}).ShouldRetry(&request.Request{
		Error: awserr.New("InvalidRequest", "invalid", nil),
	}), "another storage's retryer must not inherit configured codes")
}

func TestEmptyAdditionalRetryCodesPreserveConfigEncoding(t *testing.T) {
	for _, codes := range []map[string]struct{}{nil, parseErrorCodes(" , ")} {
		data, err := json.Marshal(&Config{RetryableErrorCodes: codes})
		require.NoError(t, err)
		assert.NotContains(t, string(data), "RetryableErrorCodes", "keep the existing config hash when unset")
	}
}
