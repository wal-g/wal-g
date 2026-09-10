package testtools

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
)

// Mock out S3 client. Implements walgs3.API.
type MockS3Client struct {
	err      bool
	notFound bool
}

func NewMockS3Client(err, notFound bool) *MockS3Client {
	return &MockS3Client{err: err, notFound: notFound}
}

var _ walgs3.API = (*MockS3Client)(nil)

func (client *MockS3Client) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input,
	_ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if client.err {
		return nil, errors.New("mock ListObjectsV2 error")
	}

	return &s3.ListObjectsV2Output{
		Contents: fakeContents(),
		Name:     input.Bucket,
	}, nil
}

func (client *MockS3Client) ListObjects(_ context.Context, input *s3.ListObjectsInput,
	_ ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	if client.err {
		return nil, errors.New("mock ListObjects error")
	}

	return &s3.ListObjectsOutput{
		Contents: fakeContents(),
		Name:     input.Bucket,
	}, nil
}

func (client *MockS3Client) ListObjectVersions(_ context.Context, input *s3.ListObjectVersionsInput,
	_ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	return &s3.ListObjectVersionsOutput{Name: input.Bucket}, nil
}

func (client *MockS3Client) GetObject(_ context.Context, _ *s3.GetObjectInput,
	_ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if client.err {
		return nil, errors.New("mock GetObject error")
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader("mock content")),
	}, nil
}

func (client *MockS3Client) HeadObject(_ context.Context, _ *s3.HeadObjectInput,
	_ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if client.err {
		return nil, errors.New("mock HeadObject error")
	} else if client.notFound {
		return nil, &types.NotFound{}
	}

	return &s3.HeadObjectOutput{}, nil
}

func (client *MockS3Client) CopyObject(_ context.Context, _ *s3.CopyObjectInput,
	_ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	return &s3.CopyObjectOutput{}, nil
}

func (client *MockS3Client) DeleteObjects(_ context.Context, _ *s3.DeleteObjectsInput,
	_ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{}, nil
}

func (client *MockS3Client) GetBucketVersioning(_ context.Context, _ *s3.GetBucketVersioningInput,
	_ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{}, nil
}

func (client *MockS3Client) GetBucketLocation(_ context.Context, _ *s3.GetBucketLocationInput,
	_ ...func(*s3.Options)) (*s3.GetBucketLocationOutput, error) {
	return &s3.GetBucketLocationOutput{}, nil
}

// Creates 5 fake S3 objects with Key and LastModified field.
func fakeContents() []types.Object {
	return []types.Object{
		{
			Key:          aws.String("mockServer/base_backup/second.nop"),
			LastModified: aws.Time(time.Date(2017, 2, 2, 30, 48, 39, 651387233, time.UTC)),
		},
		{
			Key:          aws.String("mockServer/base_backup/fourth.nop"),
			LastModified: aws.Time(time.Date(2009, 2, 27, 20, 8, 33, 651387235, time.UTC)),
		},
		{
			Key:          aws.String("mockServer/base_backup/fifth.nop"),
			LastModified: aws.Time(time.Date(2008, 11, 20, 16, 34, 58, 651387232, time.UTC)),
		},
		{
			Key:          aws.String("mockServer/base_backup/first.nop"),
			LastModified: aws.Time(time.Date(2020, 11, 31, 20, 3, 58, 651387237, time.UTC)),
		},
		{
			Key:          aws.String("mockServer/base_backup/third.nop"),
			LastModified: aws.Time(time.Date(2009, 3, 13, 4, 2, 42, 651387234, time.UTC)),
		},
	}
}
