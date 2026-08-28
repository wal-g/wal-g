package s3

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

type copyS3Mock struct {
	API

	mu sync.Mutex

	headSize      int64
	headETag      string
	headVersionID string
	copyInput     *awss3.CopyObjectInput
	createInput   *awss3.CreateMultipartUploadInput
	partInputs    []*awss3.UploadPartCopyInput
	completeInput *awss3.CompleteMultipartUploadInput
	abortInput    *awss3.AbortMultipartUploadInput
	failPart      int32
	failComplete  bool
}

func (mock *copyS3Mock) HeadObject(
	context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options),
) (*awss3.HeadObjectOutput, error) {
	return &awss3.HeadObjectOutput{
		ContentLength: aws.Int64(mock.headSize),
		ETag:          aws.String(mock.headETag),
		VersionId:     aws.String(mock.headVersionID),
	}, nil
}

func (mock *copyS3Mock) CopyObject(
	_ context.Context, input *awss3.CopyObjectInput, _ ...func(*awss3.Options),
) (*awss3.CopyObjectOutput, error) {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	mock.copyInput = input
	return &awss3.CopyObjectOutput{}, nil
}

func (mock *copyS3Mock) CreateMultipartUpload(
	_ context.Context, input *awss3.CreateMultipartUploadInput, _ ...func(*awss3.Options),
) (*awss3.CreateMultipartUploadOutput, error) {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	mock.createInput = input
	return &awss3.CreateMultipartUploadOutput{UploadId: aws.String("upload-id")}, nil
}

func (mock *copyS3Mock) UploadPartCopy(
	_ context.Context, input *awss3.UploadPartCopyInput, _ ...func(*awss3.Options),
) (*awss3.UploadPartCopyOutput, error) {
	mock.mu.Lock()
	mock.partInputs = append(mock.partInputs, input)
	mock.mu.Unlock()
	if aws.ToInt32(input.PartNumber) == mock.failPart {
		return nil, errors.New("part failed")
	}
	etag := fmt.Sprintf("etag-%d", aws.ToInt32(input.PartNumber))
	return &awss3.UploadPartCopyOutput{CopyPartResult: &types.CopyPartResult{ETag: aws.String(etag)}}, nil
}

func (mock *copyS3Mock) CompleteMultipartUpload(
	_ context.Context, input *awss3.CompleteMultipartUploadInput, _ ...func(*awss3.Options),
) (*awss3.CompleteMultipartUploadOutput, error) {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	mock.completeInput = input
	if mock.failComplete {
		return nil, errors.New("complete failed")
	}
	return &awss3.CompleteMultipartUploadOutput{}, nil
}

func (mock *copyS3Mock) AbortMultipartUpload(
	_ context.Context, input *awss3.AbortMultipartUploadInput, _ ...func(*awss3.Options),
) (*awss3.AbortMultipartUploadOutput, error) {
	mock.mu.Lock()
	defer mock.mu.Unlock()
	mock.abortInput = input
	return &awss3.AbortMultipartUploadOutput{}, nil
}

func newCopyTestFolder(
	mock API,
	bucket, root, endpoint string,
	partSize, concurrency int,
	serverSideEncryption, customerKey, kmsKey string,
) *Folder {
	uploader := NewUploader(nil, serverSideEncryption, customerKey, kmsKey, "STANDARD", "GOVERNANCE", -1)
	config := &Config{
		Endpoint: endpoint,
		Bucket:   bucket,
		RootPath: root,
		Uploader: &UploaderConfig{
			MaxPartSize:                  partSize,
			UploadConcurrency:            concurrency,
			StorageClass:                 "STANDARD",
			ServerSideEncryption:         serverSideEncryption,
			ServerSideEncryptionCustomer: customerKey,
			ServerSideEncryptionKMSID:    kmsKey,
			RetentionPeriod:              -1,
		},
	}
	return NewFolder(mock, uploader, root, config)
}

func TestCopyObjectFromUsesSingleCopyAndIndependentSSEC(t *testing.T) {
	mock := &copyS3Mock{headSize: 1024, headETag: `"etag"`, headVersionID: "v+1"}
	source := newCopyTestFolder(mock, "source-bucket", "source-root", "http://s3:9000", 5<<20, 2,
		"AES256", "source-key", "")
	destination := newCopyTestFolder(mock, "destination-bucket", "destination-root", "http://s3:9000", 5<<20, 2,
		"AES256", "destination-key", "")

	require.True(t, destination.CanCopyFrom(source))
	require.NoError(t, destination.CopyObjectFrom(t.Context(), source, "dir/a b", "copied/a b", 1024))
	require.NotNil(t, mock.copyInput)
	require.Equal(t,
		url.PathEscape("source-bucket/source-root/dir/a b")+"?versionId=v%2B1",
		aws.ToString(mock.copyInput.CopySource))
	require.Equal(t, "destination-root/copied/a b", aws.ToString(mock.copyInput.Key))
	require.Equal(t, `"etag"`, aws.ToString(mock.copyInput.CopySourceIfMatch))
	require.Equal(t, types.MetadataDirectiveReplace, mock.copyInput.MetadataDirective)
	require.Empty(t, mock.copyInput.Metadata)
	require.Equal(t, types.TaggingDirectiveReplace, mock.copyInput.TaggingDirective)
	require.Nil(t, mock.copyInput.Tagging)
	require.Equal(t, sseCustomerKeyB64("source-key"), aws.ToString(mock.copyInput.CopySourceSSECustomerKey))
	require.Equal(t, sseCustomerKeyB64("destination-key"), aws.ToString(mock.copyInput.SSECustomerKey))
}

func TestCopyObjectFromUsesMultipartCopyAtConfiguredPartSize(t *testing.T) {
	const partSize = 5 << 20
	const objectSize = 12 << 20
	mock := &copyS3Mock{headSize: objectSize, headETag: `"etag"`}
	source := newCopyTestFolder(mock, "source", "from", "http://s3:9000", partSize, 2, "", "", "")
	destination := newCopyTestFolder(mock, "destination", "to", "http://s3:9000", partSize, 2, "aws:kms", "", "kms-id")

	require.NoError(t, destination.CopyObjectFrom(t.Context(), source, "object", "object", objectSize))
	require.Nil(t, mock.copyInput)
	require.NotNil(t, mock.createInput)
	require.Empty(t, mock.createInput.Metadata)
	require.Nil(t, mock.createInput.Tagging)
	require.Equal(t, types.ServerSideEncryptionAwsKms, mock.createInput.ServerSideEncryption)
	require.Equal(t, "kms-id", aws.ToString(mock.createInput.SSEKMSKeyId))
	require.Len(t, mock.partInputs, 3)

	ranges := make(map[int32]string, len(mock.partInputs))
	for _, input := range mock.partInputs {
		ranges[aws.ToInt32(input.PartNumber)] = aws.ToString(input.CopySourceRange)
	}
	require.Equal(t, map[int32]string{
		1: "bytes=0-5242879",
		2: "bytes=5242880-10485759",
		3: "bytes=10485760-12582911",
	}, ranges)

	require.NotNil(t, mock.completeInput)
	parts := mock.completeInput.MultipartUpload.Parts
	require.Len(t, parts, 3)
	for index, part := range parts {
		require.Equal(t, int32(index+1), aws.ToInt32(part.PartNumber))
		require.Equal(t, fmt.Sprintf("etag-%d", index+1), aws.ToString(part.ETag))
	}
	require.Nil(t, mock.abortInput)
}

func TestCopyObjectFromAbortsFailedMultipartCopy(t *testing.T) {
	const partSize = 5 << 20
	mock := &copyS3Mock{headSize: 12 << 20, headETag: `"etag"`, failPart: 2}
	source := newCopyTestFolder(mock, "source", "from", "http://s3:9000", partSize, 1, "", "", "")
	destination := newCopyTestFolder(mock, "destination", "to", "http://s3:9000", partSize, 1, "", "", "")

	require.ErrorContains(t,
		destination.CopyObjectFrom(t.Context(), source, "object", "object", 12<<20),
		"copy part 2")
	require.NotNil(t, mock.abortInput)
	require.Nil(t, mock.completeInput)
}

func TestCopyObjectFromUsesMultipartAboveSingleCopyLimit(t *testing.T) {
	size := maxSingleCopySize + 1
	mock := &copyS3Mock{headSize: size, headETag: `"etag"`}
	source := newCopyTestFolder(mock, "source", "from", "http://s3:9000", int(maxCopyPartSize), 1, "", "", "")
	destination := newCopyTestFolder(mock, "destination", "to", "http://s3:9000", int(maxCopyPartSize), 1, "", "", "")

	require.NoError(t, destination.CopyObjectFrom(t.Context(), source, "object", "object", size))
	require.Nil(t, mock.copyInput)
	require.Len(t, mock.partInputs, 2)
}

func TestCopyObjectFromAbortsFailedCompletion(t *testing.T) {
	const partSize = 5 << 20
	mock := &copyS3Mock{headSize: 6 << 20, headETag: `"etag"`, failComplete: true}
	source := newCopyTestFolder(mock, "source", "from", "http://s3:9000", partSize, 1, "", "", "")
	destination := newCopyTestFolder(mock, "destination", "to", "http://s3:9000", partSize, 1, "", "", "")

	require.ErrorContains(t,
		destination.CopyObjectFrom(t.Context(), source, "object", "object", 6<<20),
		"complete multipart copy")
	require.NotNil(t, mock.abortInput)
}

func TestMultipartCopyLayoutAdaptsToMaximumPartCount(t *testing.T) {
	configured := int64(manager.MinUploadPartSize)
	size := configured*int64(manager.MaxUploadParts) + 1
	partSize, partCount, err := multipartCopyLayout(size, configured)
	require.NoError(t, err)
	require.Greater(t, partSize, configured)
	require.LessOrEqual(t, partCount, int(manager.MaxUploadParts))
}

func TestCanCopyFromRejectsDifferentEndpoints(t *testing.T) {
	mock := &copyS3Mock{}
	source := newCopyTestFolder(mock, "source", "", "http://s3:9000", 5<<20, 1, "", "", "")
	destination := newCopyTestFolder(mock, "destination", "", "http://other:9000", 5<<20, 1, "", "", "")
	require.False(t, destination.CanCopyFrom(source))
}
