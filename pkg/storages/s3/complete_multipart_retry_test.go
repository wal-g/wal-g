package s3

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalgRetryablesMalformedXML(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		query  string
		want   aws.Ternary
	}{
		{"complete multipart upload", http.MethodPost, "uploadId=abc", aws.TrueTernary},
		{"upload part", http.MethodPut, "partNumber=1&uploadId=abc", aws.UnknownTernary},
		{"initiate multipart upload", http.MethodPost, "uploads", aws.UnknownTernary},
		{"other request", http.MethodPut, "", aws.UnknownTernary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{Method: tc.method, URL: &url.URL{RawQuery: tc.query}}
			respErr := &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{
					StatusCode: http.StatusBadRequest,
					Request:    req,
				}},
				Err: &smithy.GenericAPIError{Code: "MalformedXML", Message: "The XML you provided was not well-formed"},
			}
			assert.Equal(t, tc.want, walgRetryables{}.IsErrorRetryable(respErr))
		})
	}
}

func TestCompleteMultipartUploadRetriesMalformedXML(t *testing.T) {
	const partSize = 5 * 1024 * 1024
	for _, tc := range []struct {
		name         string
		failures     int // leading failing attempts of the failing request
		failPart     bool
		wantErr      bool
		wantComplete int
		wantPart     int
		wantAbort    int
	}{
		{"complete succeeds on second attempt", 1, false, false, 2, 2, 0},
		{"complete keeps failing, upload is aborted", 100, false, true, 3, 2, 1},
		{"upload part is not retried", 100, true, true, 0, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var complete, parts, aborts, failed int
			client := awss3.NewFromConfig(aws.Config{
				Region:      "us-east-1",
				Credentials: credentials.NewStaticCredentialsProvider("key", "secret", ""),
				Retryer: newRetryerFunc(&Config{
					MaxRetries:              2,
					MaxThrottlingRetryDelay: time.Nanosecond,
				}),
				HTTPClient: smithyhttp.ClientDoFunc(func(req *http.Request) (*http.Response, error) {
					if req.Body != nil {
						if _, err := io.Copy(io.Discard, req.Body); err != nil {
							return nil, err
						}
					}
					status, header, body := http.StatusOK, http.Header{}, ""
					malformed := func() {
						if failed < tc.failures {
							failed++
							status, body = http.StatusBadRequest,
								s3ErrorXML("MalformedXML", "The XML you provided was not well-formed")
						}
					}
					q := req.URL.Query()
					switch {
					case q.Has("uploads"):
						body = "<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>"
					case q.Has("partNumber"):
						parts++
						header.Set("ETag", "\"part-etag\"")
						if tc.failPart {
							malformed()
						}
					case req.Method == http.MethodPost && q.Has("uploadId"):
						complete++
						body = "<CompleteMultipartUploadResult><ETag>\"object-etag\"</ETag></CompleteMultipartUploadResult>"
						if !tc.failPart {
							malformed()
						}
					case req.Method == http.MethodDelete:
						aborts++
						status = http.StatusNoContent
					default:
						return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL)
					}
					return &http.Response{StatusCode: status, Header: header,
						Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
				}),
			}, func(o *awss3.Options) {
				o.BaseEndpoint = aws.String("https://s3.example.com")
				o.UsePathStyle = true
			})
			uploader := CreateUploaderAPI(client, partSize, 1)
			_, err := uploader.Upload(t.Context(), &awss3.PutObjectInput{
				Bucket: aws.String("bucket"), Key: aws.String("object"),
				Body: strings.NewReader(strings.Repeat("x", partSize+1)),
			})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantComplete, complete, "CompleteMultipartUpload attempts")
			assert.Equal(t, tc.wantPart, parts, "UploadPart requests")
			assert.Equal(t, tc.wantAbort, aborts, "AbortMultipartUpload requests")
		})
	}
}
