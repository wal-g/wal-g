package s3

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

// Exercise the real SDK manager, including the 500 retry, NoSuchUpload error
// wrapping and abort-on-error, against a simulated S3 transport.
func TestReconcileCompletedMultipartUpload(t *testing.T) {
	for _, tc := range []struct {
		name        string
		enabled     bool
		headStatus  int
		stale       bool
		failPart    bool
		wantSuccess bool
	}{
		{"completed", true, 200, false, false, true},
		{"disabled", false, 200, false, false, false},
		{"missing", true, 404, false, false, false},
		{"forbidden", true, 403, false, false, false},
		{"stale", true, 200, true, false, false},
		{"missing metadata", true, 200, true, false, false},
		{"missing during part", true, 200, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var token string
			completeCalls, headCalls := 0, 0
			client := awss3.NewFromConfig(aws.Config{
				Region:                     "us-east-1",
				Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
				Retryer:                    newRetryerFunc(&Config{MaxRetries: 1, MaxThrottlingRetryDelay: time.Nanosecond}),
				RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
				HTTPClient: smithyhttp.ClientDoFunc(func(req *http.Request) (*http.Response, error) {
					status, body := 200, ""
					headers := http.Header{}
					if req.Body != nil {
						_, err := io.Copy(io.Discard, req.Body)
						require.NoError(t, err)
						require.NoError(t, req.Body.Close())
					}
					switch {
					case req.Method == "POST" && req.URL.Query().Has("uploads"):
						token = req.Header.Get("X-Amz-Meta-Walg-Upload-Attempt")
						require.Equal(t, tc.enabled, token != "")
						body = `<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>prefix/object</Key><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>`
					case req.Method == "PUT":
						headers.Set("ETag", `"part"`)
						if tc.failPart {
							status = 404
							body = s3ErrorXML("NoSuchUpload", "absent")
						}
					case req.Method == "POST":
						completeCalls++
						if completeCalls == 1 {
							status = 500
							body = s3ErrorXML("InternalError", "lost success")
						} else {
							status = 404
							body = s3ErrorXML("NoSuchUpload", "completed")
						}
					case req.Method == "DELETE":
						status = 204
					case req.Method == "HEAD":
						headCalls++
						require.Equal(t, "/prefix/object", req.URL.Path)
						require.Equal(t, "bucket.s3.us-east-1.amazonaws.com", req.URL.Host)
						require.Equal(t, "AES256", req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"))
						require.Equal(t, sseCustomerKeyB64("customer-key"), req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key"))
						status = tc.headStatus
						value := token
						if tc.stale {
							value = "previous-attempt"
							if tc.name == "missing metadata" {
								value = ""
							}
						}
						headers.Set("X-Amz-Meta-Walg-Upload-Attempt", value)
					default:
						t.Errorf("unexpected request %s", req.Method)
					}
					return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
				}),
			})
			uploader, err := createUploader(client, &UploaderConfig{
				UploadConcurrency: 1, MaxPartSize: 5 << 20, RetentionPeriod: -1,
				ServerSideEncryption: "AES256", ServerSideEncryptionCustomer: "customer-key",
				VerifyUploadOnNoSuchUpload: tc.enabled,
			})
			require.NoError(t, err)
			err = uploader.upload(t.Context(), "bucket", "prefix/object", bytes.NewReader(make([]byte, (5<<20)+1)))
			if tc.wantSuccess {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if tc.enabled && !tc.failPart {
				require.Equal(t, 1, headCalls)
			} else {
				require.Zero(t, headCalls)
			}
			if !tc.failPart {
				require.Equal(t, 2, completeCalls)
			}
		})
	}
}

func TestMissingCompletedUploadClassification(t *testing.T) {
	for _, tc := range []struct {
		operation, code string
		status          int
		want            bool
	}{
		{"CompleteMultipartUpload", "NoSuchUpload", 404, true},
		{"UploadPart", "NoSuchUpload", 404, false},
		{"CompleteMultipartUpload", "NotFound", 404, false},
		{"CompleteMultipartUpload", "NoSuchUpload", 500, false},
	} {
		err := &smithy.OperationError{ServiceID: "S3", OperationName: tc.operation, Err: s3ResponseError(tc.status, &smithy.GenericAPIError{Code: tc.code})}
		require.Equal(t, tc.want, isMissingCompletedUpload(err))
	}
	require.False(t, isMissingCompletedUpload(nil))
}
