package s3

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
)

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

	assert.Equal(t, uploadInput.Bucket, aws.String(dummyBucket))
	assert.Equal(t, uploadInput.Key, aws.String(dummyPath))
	assert.Equal(t, uploadInput.Body, dummyContent)
	assert.Equal(t, uploadInput.StorageClass, aws.String(dummyStorageClass))
}

func TestCreateUploadInput_WithServerSideEncryptionAndWithCustomerKey(t *testing.T) {
	uploader := &Uploader{
		StorageClass:         dummyStorageClass,
		serverSideEncryption: dummyServerSideEncryption,
		SSECustomerKey:       dummySSECustomerKey,
	}

	uploadInput := uploader.createUploadInput(dummyBucket, dummyPath, dummyContent)

	assert.Equal(t, uploadInput.Bucket, aws.String(dummyBucket))
	assert.Equal(t, uploadInput.Key, aws.String(dummyPath))
	assert.Equal(t, uploadInput.Body, dummyContent)
	assert.Equal(t, uploadInput.StorageClass, aws.String(dummyStorageClass))
	assert.Equal(t, uploadInput.SSECustomerAlgorithm, aws.String(dummyServerSideEncryption))
	assert.Equal(t, uploadInput.SSECustomerKey, aws.String(dummySSECustomerKey))

	hash := md5.Sum([]byte(uploader.SSECustomerKey))
	assert.Equal(t, uploadInput.SSECustomerKeyMD5, aws.String(base64.StdEncoding.EncodeToString(hash[:])))
}

func TestCreateUploadInput_WithServerSideEncryptionAndWithoutCustomerKey(t *testing.T) {
	uploader := &Uploader{
		StorageClass:         dummyStorageClass,
		serverSideEncryption: dummyServerSideEncryption,
	}

	uploadInput := uploader.createUploadInput(dummyBucket, dummyPath, dummyContent)

	assert.Equal(t, uploadInput.Bucket, aws.String(dummyBucket))
	assert.Equal(t, uploadInput.Key, aws.String(dummyPath))
	assert.Equal(t, uploadInput.Body, dummyContent)
	assert.Equal(t, uploadInput.StorageClass, aws.String(dummyStorageClass))
	assert.Equal(t, uploadInput.ServerSideEncryption, aws.String(dummyServerSideEncryption))
}

func TestCreateUploadInput_WithServerSideEncryptionAndWithKMSKeyID(t *testing.T) {
	uploader := &Uploader{
		StorageClass:         dummyStorageClass,
		serverSideEncryption: dummyServerSideEncryption,
		SSEKMSKeyID:          dummySSEKMSKeyID,
	}

	uploadInput := uploader.createUploadInput(dummyBucket, dummyPath, dummyContent)

	assert.Equal(t, uploadInput.Bucket, aws.String(dummyBucket))
	assert.Equal(t, uploadInput.Key, aws.String(dummyPath))
	assert.Equal(t, uploadInput.Body, dummyContent)
	assert.Equal(t, uploadInput.StorageClass, aws.String(dummyStorageClass))
	assert.Equal(t, uploadInput.SSEKMSKeyId, aws.String(dummySSEKMSKeyID))
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
