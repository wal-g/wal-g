package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type multipartChecksumClient struct {
	createHeader http.Header
	partHeaders  []http.Header
}

func (c *multipartChecksumClient) Do(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		if _, err := io.Copy(io.Discard, req.Body); err != nil {
			return nil, err
		}
	}
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Request:    req,
	}
	var body string
	switch {
	case req.Method == http.MethodPost && req.URL.Query().Has("uploads"):
		c.createHeader = req.Header.Clone()
		body = `<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>`
	case req.Method == http.MethodPut && req.URL.Query().Has("partNumber"):
		c.partHeaders = append(c.partHeaders, req.Header.Clone())
		response.Header.Set("ETag", `"test-etag"`)
		if req.Header.Get("X-Amz-Sdk-Checksum-Algorithm") != "" {
			response.Header.Set("X-Amz-Checksum-Crc32", "AAAAAA==")
		}
	case req.Method == http.MethodPost && req.URL.Query().Has("uploadId"):
		body = `<CompleteMultipartUploadResult><ETag>"test-etag"</ETag></CompleteMultipartUploadResult>`
	default:
		return nil, fmt.Errorf("unexpected multipart request: %s %s", req.Method, req.URL)
	}
	response.Body = io.NopCloser(strings.NewReader(body))
	return response, nil
}

func TestMultipartUploadRespectsClientChecksumPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy aws.RequestChecksumCalculation
		crc32  bool
	}{
		{"required", aws.RequestChecksumCalculationWhenRequired, false},
		{"supported", aws.RequestChecksumCalculationWhenSupported, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &multipartChecksumClient{}
			client := s3.NewFromConfig(aws.Config{
				Region:                     "us-east-1",
				Credentials:                credentials.NewStaticCredentialsProvider("key", "secret", ""),
				HTTPClient:                 capture,
				RequestChecksumCalculation: tc.policy,
			}, func(o *s3.Options) {
				o.BaseEndpoint = aws.String("https://s3.example.com")
				o.UsePathStyle = true
			})
			const partSize = 5 << 20
			// One worker keeps capture access sequential. A non-seekable body
			// larger than one part exercises the streaming backup upload path.
			uploader := CreateUploaderAPI(client, partSize, 1)
			_, err := uploader.Upload(t.Context(), &s3.PutObjectInput{
				Bucket: aws.String("bucket"),
				Key:    aws.String("backup.tar"),
				Body:   bytes.NewBuffer(make([]byte, partSize+1)),
			})
			require.NoError(t, err)
			require.Len(t, capture.partHeaders, 2)
			if tc.crc32 {
				assert.Equal(t, "CRC32", capture.createHeader.Get("X-Amz-Checksum-Algorithm"))
			} else {
				assert.Empty(t, capture.createHeader.Get("X-Amz-Checksum-Algorithm"))
			}
			for _, header := range capture.partHeaders {
				if tc.crc32 {
					assert.Equal(t, "CRC32", header.Get("X-Amz-Sdk-Checksum-Algorithm"))
				} else {
					assert.Equal(t, "UNSIGNED-PAYLOAD", header.Get("X-Amz-Content-Sha256"))
					assert.Empty(t, header.Get("X-Amz-Sdk-Checksum-Algorithm"))
					assert.Empty(t, header.Get("X-Amz-Trailer"))
					assert.Empty(t, header.Get("X-Amz-Checksum-Crc32"))
					assert.NotContains(t, header.Get("Content-Encoding"), "aws-chunked")
				}
			}
		})
	}
}

var (
	dummyStorageClass         = "dummyStorageClass"
	dummyServerSideEncryption = "dummyServerSideEncryption"
	dummySSECustomerKey       = "dummyKey"
	dummySSEKMSKeyID          = "dummyKeyId"
	dummyBucket               = "dummyBucket"
	dummyPath                 = "dummyPath"
	dummyContent              = strings.NewReader("dummyContent")
)

func TestCreateUploadInput_WithoutServerSideEncryption(t *testing.T) {
	uploader := &Uploader{
		StorageClass:         dummyStorageClass,
		serverSideEncryption: "",
	}

	uploadInput := uploader.createUploadInput(dummyBucket, dummyPath, dummyContent)

	assert.Equal(t, aws.String(dummyBucket), uploadInput.Bucket)
	assert.Equal(t, aws.String(dummyPath), uploadInput.Key)
	assert.Equal(t, dummyContent, uploadInput.Body)
	assert.Equal(t, types.StorageClass(dummyStorageClass), uploadInput.StorageClass)
}

func TestCreateUploadInput_WithServerSideEncryptionAndWithCustomerKey(t *testing.T) {
	uploader := &Uploader{
		StorageClass:         dummyStorageClass,
		serverSideEncryption: dummyServerSideEncryption,
		SSECustomerKey:       dummySSECustomerKey,
	}

	uploadInput := uploader.createUploadInput(dummyBucket, dummyPath, dummyContent)

	assert.Equal(t, aws.String(dummyBucket), uploadInput.Bucket)
	assert.Equal(t, aws.String(dummyPath), uploadInput.Key)
	assert.Equal(t, dummyContent, uploadInput.Body)
	assert.Equal(t, types.StorageClass(dummyStorageClass), uploadInput.StorageClass)
	assert.Equal(t, aws.String(dummyServerSideEncryption), uploadInput.SSECustomerAlgorithm)
	// v2 requires the caller to base64-encode the SSE-C key; v1 did this internally.
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString([]byte(dummySSECustomerKey))), uploadInput.SSECustomerKey)

	hash := md5.Sum([]byte(uploader.SSECustomerKey))
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(hash[:])), uploadInput.SSECustomerKeyMD5)
}

func TestCreateUploadInput_WithServerSideEncryptionAndWithoutCustomerKey(t *testing.T) {
	uploader := &Uploader{
		StorageClass:         dummyStorageClass,
		serverSideEncryption: dummyServerSideEncryption,
	}

	uploadInput := uploader.createUploadInput(dummyBucket, dummyPath, dummyContent)

	assert.Equal(t, aws.String(dummyBucket), uploadInput.Bucket)
	assert.Equal(t, aws.String(dummyPath), uploadInput.Key)
	assert.Equal(t, dummyContent, uploadInput.Body)
	assert.Equal(t, types.StorageClass(dummyStorageClass), uploadInput.StorageClass)
	assert.Equal(t, types.ServerSideEncryption(dummyServerSideEncryption), uploadInput.ServerSideEncryption)
}

func TestCreateUploadInput_WithServerSideEncryptionAndWithKMSKeyID(t *testing.T) {
	uploader := &Uploader{
		StorageClass:         dummyStorageClass,
		serverSideEncryption: dummyServerSideEncryption,
		SSEKMSKeyID:          dummySSEKMSKeyID,
	}

	uploadInput := uploader.createUploadInput(dummyBucket, dummyPath, dummyContent)

	assert.Equal(t, aws.String(dummyBucket), uploadInput.Bucket)
	assert.Equal(t, aws.String(dummyPath), uploadInput.Key)
	assert.Equal(t, dummyContent, uploadInput.Body)
	assert.Equal(t, types.StorageClass(dummyStorageClass), uploadInput.StorageClass)
	assert.Equal(t, aws.String(dummySSEKMSKeyID), uploadInput.SSEKMSKeyId)
}

func TestPartitionStrings(t *testing.T) {
	testCases := []struct {
		strings   []string
		blockSize int
		expected  [][]string
	}{
		{[]string{"1", "2", "3", "4", "5"}, 2, [][]string{{"1", "2"}, {"3", "4"}, {"5"}}},
		{[]string{"1", "2", "3", "4", "5", "6"}, 3, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}},
		{[]string{"1", "2", "3", "4", "5"}, 1000, [][]string{{"1", "2", "3", "4", "5"}}},
		{[]string{"1", "2", "3", "4", "5"}, 1, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}}},
		{[]string{"1", "2", "3", "4", "5"}, 0, [][]string{{"1", "2", "3", "4", "5"}}},
		{[]string{"1", "2", "3", "4", "5"}, -1, [][]string{{"1", "2", "3", "4", "5"}}},
		{[]string{"1", "2"}, 5, [][]string{{"1", "2"}}},
		{[]string{"1"}, 1, [][]string{{"1"}}},
		{[]string{}, 1, [][]string{}},
		{[]string{"foo", "bar", "baz"}, 4, [][]string{{"foo", "bar", "baz"}}},
		{[]string{"foo", "bar", "baz"}, 3, [][]string{{"foo", "bar", "baz"}}},
		{[]string{"foo", "bar", "baz"}, 2, [][]string{{"foo", "bar"}, {"baz"}}},
		{[]string{"foo", "bar", "baz"}, 1, [][]string{{"foo"}, {"bar"}, {"baz"}}},
		{[]string{"foo", "bar", "baz"}, 0, [][]string{{"foo", "bar", "baz"}}},
		{[]string{"foo", "bar", "baz"}, -1, [][]string{{"foo", "bar", "baz"}}},
		{
			[]string{
				"This is a long string that contains a lot of words and characters for testing purposes.",
				"The quick brown fox jumps over the lazy dog",
				"Lorem ipsum dolor sit amet, consectetur adipiscing elit",
				"Hello, World!",
				" ",
				"",
			},
			2,
			[][]string{
				{
					"This is a long string that contains a lot of words and characters for testing purposes.",
					"The quick brown fox jumps over the lazy dog",
				},
				{
					"Lorem ipsum dolor sit amet, consectetur adipiscing elit",
					"Hello, World!",
				},
				{
					" ",
					"",
				},
			},
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual := partitionStrings(tc.strings, tc.blockSize)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestPartitionObjects(t *testing.T) {
	testCases := []struct {
		strings   []storage.Object
		blockSize int
		expected  [][]storage.Object
	}{
		{[]storage.Object{nil, nil, nil, nil, nil}, 2, [][]storage.Object{{nil, nil}, {nil, nil}, {nil}}},
		{[]storage.Object{nil, nil, nil, nil, nil, nil}, 3, [][]storage.Object{{nil, nil, nil}, {nil, nil, nil}}},
		{[]storage.Object{nil, nil, nil, nil, nil}, 1000, [][]storage.Object{{nil, nil, nil, nil, nil}}},
		{[]storage.Object{nil, nil, nil, nil, nil}, 1, [][]storage.Object{{nil}, {nil}, {nil}, {nil}, {nil}}},
		{[]storage.Object{nil, nil, nil, nil, nil}, 0, [][]storage.Object{{nil, nil, nil, nil, nil}}},
		{[]storage.Object{nil, nil, nil, nil, nil}, -1, [][]storage.Object{{nil, nil, nil, nil, nil}}},
		{[]storage.Object{nil, nil}, 5, [][]storage.Object{{nil, nil}}},
		{[]storage.Object{nil}, 1, [][]storage.Object{{nil}}},
		{[]storage.Object{}, 1, [][]storage.Object{}},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual := partitionObjects(tc.strings, tc.blockSize)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
