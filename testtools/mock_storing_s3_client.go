package testtools

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/wal-g/wal-g"
	"io/ioutil"
)

type mockStoringS3Client struct {
	s3iface.S3API
	storage MockStorage
}

func NewMockStoringS3Client(storage MockStorage) *mockStoringS3Client {
	return &mockStoringS3Client{storage: storage}
}

func (client *mockStoringS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	buffer, ok := client.storage[*input.Bucket+*input.Key]
	if !ok {
		return nil, awserr.New(walg.NoSuchKeyAWSErrorCode, "", nil)
	} else {
		output := &s3.GetObjectOutput{
			Body: ioutil.NopCloser(&buffer),
		}
		return output, nil
	}
}
