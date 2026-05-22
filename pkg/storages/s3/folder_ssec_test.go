package s3_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

type MockS3ClientSSEC struct {
	s3iface.S3API
	LastGetObjectInput  *s3.GetObjectInput
	LastHeadObjectInput *s3.HeadObjectInput
	LastCopyObjectInput *s3.CopyObjectInput
}

func (m *MockS3ClientSSEC) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	m.LastGetObjectInput = input
	output := &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader("mock encrypted content")),
	}
	return output, nil
}

func (m *MockS3ClientSSEC) HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	m.LastHeadObjectInput = input
	return &s3.HeadObjectOutput{}, nil
}

func (m *MockS3ClientSSEC) CopyObject(input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	m.LastCopyObjectInput = input
	return &s3.CopyObjectOutput{}, nil
}

func createSSECUploader(sseAlgorithm, sseKey string) *walgs3.Uploader {
	return walgs3.NewUploader(nil, sseAlgorithm, sseKey, "", "STANDARD", "GOVERNANCE", 0)
}

func TestReadObject_WithSSEC_AddsCorrectHeaders(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	sseKey := "MySecretKey32BytesLongForSSE!123"
	sseAlgorithm := "AES256"
	expectedMD5 := walgs3.GetSSECustomerKeyMD5(sseKey)

	uploader := createSSECUploader(sseAlgorithm, sseKey)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject("test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, mockClient.LastGetObjectInput)
	assert.NotNil(t, mockClient.LastGetObjectInput.SSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *mockClient.LastGetObjectInput.SSECustomerAlgorithm)
	assert.NotNil(t, mockClient.LastGetObjectInput.SSECustomerKey)
	assert.Equal(t, sseKey, *mockClient.LastGetObjectInput.SSECustomerKey)
	assert.NotNil(t, mockClient.LastGetObjectInput.SSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *mockClient.LastGetObjectInput.SSECustomerKeyMD5)
}

func TestReadObject_WithoutSSEC_NoHeadersAdded(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	uploader := createSSECUploader("", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject("test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, mockClient.LastGetObjectInput)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerKey)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerKeyMD5)
}

func TestExists_WithSSEC_AddsCorrectHeaders(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	sseKey := "MySecretKey32BytesLongForSSE!123"
	sseAlgorithm := "AES256"
	expectedMD5 := walgs3.GetSSECustomerKeyMD5(sseKey)

	uploader := createSSECUploader(sseAlgorithm, sseKey)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	exists, err := folder.Exists("test-file.txt")

	require.NoError(t, err)
	assert.True(t, exists)

	require.NotNil(t, mockClient.LastHeadObjectInput)
	assert.NotNil(t, mockClient.LastHeadObjectInput.SSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *mockClient.LastHeadObjectInput.SSECustomerAlgorithm)
	assert.NotNil(t, mockClient.LastHeadObjectInput.SSECustomerKey)
	assert.Equal(t, sseKey, *mockClient.LastHeadObjectInput.SSECustomerKey)
	assert.NotNil(t, mockClient.LastHeadObjectInput.SSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *mockClient.LastHeadObjectInput.SSECustomerKeyMD5)
}

func TestExists_WithoutSSEC_NoHeadersAdded(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	uploader := createSSECUploader("", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	exists, err := folder.Exists("test-file.txt")

	require.NoError(t, err)
	assert.True(t, exists)

	require.NotNil(t, mockClient.LastHeadObjectInput)
	assert.Nil(t, mockClient.LastHeadObjectInput.SSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastHeadObjectInput.SSECustomerKey)
	assert.Nil(t, mockClient.LastHeadObjectInput.SSECustomerKeyMD5)
}

func TestCopyObject_WithSSEC_AddsCorrectHeadersForSourceAndDestination(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	sseKey := "MySecretKey32BytesLongForSSE!123"
	sseAlgorithm := "AES256"
	expectedMD5 := walgs3.GetSSECustomerKeyMD5(sseKey)

	uploader := createSSECUploader(sseAlgorithm, sseKey)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject("source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, mockClient.LastCopyObjectInput)

	assert.NotNil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *mockClient.LastCopyObjectInput.CopySourceSSECustomerAlgorithm)
	assert.NotNil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKey)
	assert.Equal(t, sseKey, *mockClient.LastCopyObjectInput.CopySourceSSECustomerKey)
	assert.NotNil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *mockClient.LastCopyObjectInput.CopySourceSSECustomerKeyMD5)

	assert.NotNil(t, mockClient.LastCopyObjectInput.SSECustomerAlgorithm)
	assert.Equal(t, sseAlgorithm, *mockClient.LastCopyObjectInput.SSECustomerAlgorithm)
	assert.NotNil(t, mockClient.LastCopyObjectInput.SSECustomerKey)
	assert.Equal(t, sseKey, *mockClient.LastCopyObjectInput.SSECustomerKey)
	assert.NotNil(t, mockClient.LastCopyObjectInput.SSECustomerKeyMD5)
	assert.Equal(t, expectedMD5, *mockClient.LastCopyObjectInput.SSECustomerKeyMD5)
}

func TestCopyObject_WithoutSSEC_NoHeadersAdded(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	uploader := createSSECUploader("", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject("source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, mockClient.LastCopyObjectInput)

	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKey)
	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKeyMD5)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerKey)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerKeyMD5)
}

func TestReadObject_WithSSECButNoAlgorithm_NoHeadersAdded(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	uploader := createSSECUploader("", "MySecretKey32BytesLongForSSE!123")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject("test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, mockClient.LastGetObjectInput)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerKey)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerKeyMD5)
}

func TestReadObject_WithSSECButNoKey_NoHeadersAdded(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	uploader := createSSECUploader("AES256", "")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	reader, err := folder.ReadObject("test-file.txt")

	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	require.NotNil(t, mockClient.LastGetObjectInput)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerKey)
	assert.Nil(t, mockClient.LastGetObjectInput.SSECustomerKeyMD5)
}

func TestReadObject_WithSSEC_CorrectObjectPath(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	uploader := createSSECUploader("AES256", "MySecretKey32BytesLongForSSE!123")
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "base/path/", config)

	_, err := folder.ReadObject("subfolder/file.txt")

	require.NoError(t, err)
	require.NotNil(t, mockClient.LastGetObjectInput)
	assert.Equal(t, "base/path/subfolder/file.txt", *mockClient.LastGetObjectInput.Key)
	assert.Equal(t, "test-bucket", *mockClient.LastGetObjectInput.Bucket)
}

func TestCopyObject_WithSSEKMS_AddsCorrectHeadersForKMS(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	sseAlgorithm := "aws:kms"
	sseKMSKeyID := "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"

	uploader := walgs3.NewUploader(nil, sseAlgorithm, "", sseKMSKeyID, "STANDARD", "GOVERNANCE", 0)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject("source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, mockClient.LastCopyObjectInput)

	assert.NotNil(t, mockClient.LastCopyObjectInput.ServerSideEncryption)
	assert.Equal(t, sseAlgorithm, *mockClient.LastCopyObjectInput.ServerSideEncryption)

	assert.NotNil(t, mockClient.LastCopyObjectInput.SSEKMSKeyId)
	assert.Equal(t, sseKMSKeyID, *mockClient.LastCopyObjectInput.SSEKMSKeyId)

	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKey)
	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKeyMD5)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerKey)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerKeyMD5)
}

func TestCopyObject_WithSSES3_AddsCorrectHeadersForS3(t *testing.T) {
	mockClient := &MockS3ClientSSEC{}
	sseAlgorithm := "AES256"

	uploader := walgs3.NewUploader(nil, sseAlgorithm, "", "", "STANDARD", "GOVERNANCE", 0)
	config := &walgs3.Config{Bucket: "test-bucket"}
	folder := walgs3.NewFolder(mockClient, uploader, "test-path/", config)

	err := folder.CopyObject("source-file.txt", "dest-file.txt")

	require.NoError(t, err)
	require.NotNil(t, mockClient.LastCopyObjectInput)

	assert.NotNil(t, mockClient.LastCopyObjectInput.ServerSideEncryption)
	assert.Equal(t, sseAlgorithm, *mockClient.LastCopyObjectInput.ServerSideEncryption)

	assert.Nil(t, mockClient.LastCopyObjectInput.SSEKMSKeyId)

	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKey)
	assert.Nil(t, mockClient.LastCopyObjectInput.CopySourceSSECustomerKeyMD5)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerAlgorithm)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerKey)
	assert.Nil(t, mockClient.LastCopyObjectInput.SSECustomerKeyMD5)
}
