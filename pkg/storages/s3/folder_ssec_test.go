package s3_test

import (
	"context"
	"encoding/base64"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/test/mocks"
	"go.uber.org/mock/gomock"
)

type capturedInput[T any] struct {
	Input *T
}

func expectGetObject(mockClient *mocks.MockAPI) *capturedInput[s3.GetObjectInput] {
	captured := &capturedInput[s3.GetObjectInput]{}
	mockClient.EXPECT().GetObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			captured.Input = input
			return &s3.GetObjectOutput{
				Body: io.NopCloser(strings.NewReader("mock encrypted content")),
			}, nil
		})
	return captured
}

func expectHeadObject(mockClient *mocks.MockAPI) *capturedInput[s3.HeadObjectInput] {
	captured := &capturedInput[s3.HeadObjectInput]{}
	mockClient.EXPECT().HeadObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			captured.Input = input
			return &s3.HeadObjectOutput{}, nil
		})
	return captured
}

func expectCopyObject(mockClient *mocks.MockAPI) *capturedInput[s3.CopyObjectInput] {
	// Folder.CopyObject checks the source with Exists before copying.
	expectHeadObject(mockClient)

	captured := &capturedInput[s3.CopyObjectInput]{}
	mockClient.EXPECT().CopyObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
			captured.Input = input
			return &s3.CopyObjectOutput{}, nil
		})
	return captured
}

// expectedSSECustomerKey returns the on-the-wire SSE-C key value: v2 requires
// the caller to base64-encode the raw key (v1 did this internally).
func expectedSSECustomerKey(raw string) string {
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

func createSSECUploader(sseAlgorithm, sseKey string) *walgs3.Uploader {
	return walgs3.NewUploader(nil, sseAlgorithm, sseKey, "", "STANDARD", "GOVERNANCE", 0)
}

func TestReadObject_WithSSEC_AddsCorrectHeaders(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	getObject := expectGetObject(mockClient)
	sseKey := "MySecretKey32BytesLongForSSE!123"
	sseAlgorithm := "AES256"
	expectedMD5 := walgs3.GetSSECustomerKeyMD5(sseKey)

	uploader := createSSECUploader(sseAlgorithm, sseKey)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject(t.Context(), "test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, getObject.Input)
	assert.NotNil(t, getObject.Input.SSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *getObject.Input.SSECustomerAlgorithm)
	assert.NotNil(t, getObject.Input.SSECustomerKey)
	assert.Equal(t, expectedSSECustomerKey(sseKey), *getObject.Input.SSECustomerKey)
	assert.NotNil(t, getObject.Input.SSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *getObject.Input.SSECustomerKeyMD5)
}

func TestReadObject_WithoutSSEC_NoHeadersAdded(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	getObject := expectGetObject(mockClient)
	uploader := createSSECUploader("", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject(t.Context(), "test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, getObject.Input)
	assert.Nil(t, getObject.Input.SSECustomerAlgorithm)
	assert.Nil(t, getObject.Input.SSECustomerKey)
	assert.Nil(t, getObject.Input.SSECustomerKeyMD5)
}

func TestExists_WithSSEC_AddsCorrectHeaders(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	headObject := expectHeadObject(mockClient)
	sseKey := "MySecretKey32BytesLongForSSE!123"
	sseAlgorithm := "AES256"
	expectedMD5 := walgs3.GetSSECustomerKeyMD5(sseKey)

	uploader := createSSECUploader(sseAlgorithm, sseKey)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	exists, err := folder.Exists(t.Context(), "test-file.txt")

	require.NoError(t, err)
	assert.True(t, exists)

	require.NotNil(t, headObject.Input)
	assert.NotNil(t, headObject.Input.SSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *headObject.Input.SSECustomerAlgorithm)
	assert.NotNil(t, headObject.Input.SSECustomerKey)
	assert.Equal(t, expectedSSECustomerKey(sseKey), *headObject.Input.SSECustomerKey)
	assert.NotNil(t, headObject.Input.SSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *headObject.Input.SSECustomerKeyMD5)
}

func TestExists_WithoutSSEC_NoHeadersAdded(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	headObject := expectHeadObject(mockClient)
	uploader := createSSECUploader("", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	exists, err := folder.Exists(t.Context(), "test-file.txt")

	require.NoError(t, err)
	assert.True(t, exists)

	require.NotNil(t, headObject.Input)
	assert.Nil(t, headObject.Input.SSECustomerAlgorithm)
	assert.Nil(t, headObject.Input.SSECustomerKey)
	assert.Nil(t, headObject.Input.SSECustomerKeyMD5)
}

func TestCopyObject_WithSSEC_AddsCorrectHeadersForSourceAndDestination(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	copyObject := expectCopyObject(mockClient)
	sseKey := "MySecretKey32BytesLongForSSE!123"
	sseAlgorithm := "AES256"
	expectedMD5 := walgs3.GetSSECustomerKeyMD5(sseKey)
	encodedKey := expectedSSECustomerKey(sseKey)

	uploader := createSSECUploader(sseAlgorithm, sseKey)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject(t.Context(), "source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, copyObject.Input)

	assert.NotNil(t, copyObject.Input.CopySourceSSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *copyObject.Input.CopySourceSSECustomerAlgorithm)
	assert.NotNil(t, copyObject.Input.CopySourceSSECustomerKey)
	assert.Equal(t, encodedKey, *copyObject.Input.CopySourceSSECustomerKey)
	assert.NotNil(t, copyObject.Input.CopySourceSSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *copyObject.Input.CopySourceSSECustomerKeyMD5)

	assert.NotNil(t, copyObject.Input.SSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *copyObject.Input.SSECustomerAlgorithm)
	assert.NotNil(t, copyObject.Input.SSECustomerKey)
	assert.Equal(t, encodedKey, *copyObject.Input.SSECustomerKey)
	assert.NotNil(t, copyObject.Input.SSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *copyObject.Input.SSECustomerKeyMD5)
}

func TestCopyObject_WithoutSSEC_NoHeadersAdded(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	copyObject := expectCopyObject(mockClient)
	uploader := createSSECUploader("", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject(t.Context(), "source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, copyObject.Input)

	assert.Nil(t, copyObject.Input.CopySourceSSECustomerAlgorithm)
	assert.Nil(t, copyObject.Input.CopySourceSSECustomerKey)
	assert.Nil(t, copyObject.Input.CopySourceSSECustomerKeyMD5)
	assert.Nil(t, copyObject.Input.SSECustomerAlgorithm)
	assert.Nil(t, copyObject.Input.SSECustomerKey)
	assert.Nil(t, copyObject.Input.SSECustomerKeyMD5)
}

func TestReadObject_WithSSECButNoAlgorithm_NoHeadersAdded(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	getObject := expectGetObject(mockClient)
	uploader := createSSECUploader("", "MySecretKey32BytesLongForSSE!123")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject(t.Context(), "test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, getObject.Input)
	assert.Nil(t, getObject.Input.SSECustomerAlgorithm)
	assert.Nil(t, getObject.Input.SSECustomerKey)
	assert.Nil(t, getObject.Input.SSECustomerKeyMD5)
}

func TestReadObject_WithSSECButNoKey_NoHeadersAdded(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	getObject := expectGetObject(mockClient)
	uploader := createSSECUploader("AES256", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject(t.Context(), "test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, getObject.Input)
	assert.Nil(t, getObject.Input.SSECustomerAlgorithm)
	assert.Nil(t, getObject.Input.SSECustomerKey)
	assert.Nil(t, getObject.Input.SSECustomerKeyMD5)
}

func TestReadObject_WithSSEC_CorrectObjectPath(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	getObject := expectGetObject(mockClient)
	uploader := createSSECUploader("AES256", "MySecretKey32BytesLongForSSE!123")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "base/path/", config)

	_, err := folder.ReadObject(t.Context(), "subfolder/file.txt")

	require.NoError(t, err)
	require.NotNil(t, getObject.Input)
	assert.Equal(t, "base/path/subfolder/file.txt", *getObject.Input.Key)
	assert.Equal(t, "test-bucket", *getObject.Input.Bucket)
}

func TestCopyObject_WithSSEKMS_AddsCorrectHeadersForKMS(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	copyObject := expectCopyObject(mockClient)
	sseAlgorithm := "aws:kms"
	sseKMSKeyID := "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"

	uploader := walgs3.NewUploader(nil, sseAlgorithm, "", sseKMSKeyID, "STANDARD", "GOVERNANCE", 0)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject(t.Context(), "source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, copyObject.Input)

	// In v2, ServerSideEncryption is a typed enum (types.ServerSideEncryption),
	// so it's the string value itself, not a pointer.
	assert.Equal(t, sseAlgorithm, string(copyObject.Input.ServerSideEncryption))

	assert.NotNil(t, copyObject.Input.SSEKMSKeyId)
	assert.Equal(t, sseKMSKeyID, *copyObject.Input.SSEKMSKeyId)

	assert.Nil(t, copyObject.Input.CopySourceSSECustomerAlgorithm)
	assert.Nil(t, copyObject.Input.CopySourceSSECustomerKey)
	assert.Nil(t, copyObject.Input.CopySourceSSECustomerKeyMD5)
	assert.Nil(t, copyObject.Input.SSECustomerAlgorithm)
	assert.Nil(t, copyObject.Input.SSECustomerKey)
	assert.Nil(t, copyObject.Input.SSECustomerKeyMD5)
}

func TestCopyObject_WithSSES3_AddsCorrectHeadersForS3(t *testing.T) {
	mockClient := mocks.NewMockAPI(gomock.NewController(t))
	copyObject := expectCopyObject(mockClient)
	sseAlgorithm := "AES256"

	uploader := walgs3.NewUploader(nil, sseAlgorithm, "", "", "STANDARD", "GOVERNANCE", 0)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject(t.Context(), "source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, copyObject.Input)

	assert.Equal(t, sseAlgorithm, string(copyObject.Input.ServerSideEncryption))

	assert.Nil(t, copyObject.Input.SSEKMSKeyId)

	assert.Nil(t, copyObject.Input.CopySourceSSECustomerAlgorithm)
	assert.Nil(t, copyObject.Input.CopySourceSSECustomerKey)
	assert.Nil(t, copyObject.Input.CopySourceSSECustomerKeyMD5)
	assert.Nil(t, copyObject.Input.SSECustomerAlgorithm)
	assert.Nil(t, copyObject.Input.SSECustomerKey)
	assert.Nil(t, copyObject.Input.SSECustomerKeyMD5)
}
