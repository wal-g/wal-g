package s3

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type UploaderConfig struct {
	VerifyUploadOnNoSuchUpload   bool
	UploadConcurrency            int
	MaxPartSize                  int
	AutoPartSize                 bool
	StorageClass                 string
	ServerSideEncryption         string
	ServerSideEncryptionCustomer string
	ServerSideEncryptionKMSID    string
	RetentionPeriod              int
	RetentionMode                string
}

// UploaderAPI is the narrow caller-defined interface against v2's manager.Uploader.
// v2 has no s3manageriface package.
type UploaderAPI interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

func createUploader(s3Client *s3.Client, config *UploaderConfig) (*Uploader, error) {
	uploaderAPI := CreateUploaderAPI(s3Client, config.MaxPartSize, config.UploadConcurrency)

	if (config.ServerSideEncryption == "aws:kms") == (config.ServerSideEncryptionKMSID == "") {
		return nil, fmt.Errorf("server-side encryption KMS key ID must be set if 'aws:kms' encryption is used")
	}
	uploader := NewUploader(
		uploaderAPI,
		config.ServerSideEncryption,
		config.ServerSideEncryptionCustomer,
		config.ServerSideEncryptionKMSID,
		config.StorageClass,
		config.RetentionMode,
		config.RetentionPeriod,
	)
	uploader.verifyUploadOnNoSuchUpload = config.VerifyUploadOnNoSuchUpload
	uploader.headAPI = s3Client
	uploader.partSize = int64(config.MaxPartSize)
	uploader.autoPartSize = config.AutoPartSize
	return uploader, nil
}

type headObjectAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

const uploadAttemptMetadataKey = "walg-upload-attempt"

type Uploader struct {
	verifyUploadOnNoSuchUpload bool
	headAPI                    headObjectAPI
	uploaderAPI                UploaderAPI
	partSize                   int64
	autoPartSize               bool
	serverSideEncryption       string
	SSECustomerKey             string
	SSEKMSKeyID                string
	StorageClass               string
	RetentionMode              string
	RetentionPeriod            time.Duration
}

func NewUploader(uploaderAPI UploaderAPI, serverSideEncryption, sseCustomerKey, sseKmsKeyID, storageClass,
	retentionMode string, retentionPeriod int) *Uploader {
	if retentionMode == "" {
		retentionMode = "GOVERNANCE"
	}
	return &Uploader{
		uploaderAPI:          uploaderAPI,
		serverSideEncryption: serverSideEncryption,
		SSECustomerKey:       sseCustomerKey,
		SSEKMSKeyID:          sseKmsKeyID,
		StorageClass:         storageClass,
		RetentionMode:        retentionMode,
		RetentionPeriod:      time.Duration(retentionPeriod),
	}
}

func (uploader *Uploader) createUploadInput(bucket, path string, content io.Reader) *s3.PutObjectInput {
	uploadInput := &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(path),
		Body:         content,
		StorageClass: types.StorageClass(uploader.StorageClass),
	}
	if uploader.RetentionPeriod != defaultDisabledRetentionPeriod {
		mytime := time.Now().Add(time.Second * uploader.RetentionPeriod)
		uploadInput.ObjectLockMode = types.ObjectLockMode(uploader.RetentionMode)
		uploadInput.ObjectLockRetainUntilDate = &mytime
	}

	if uploader.serverSideEncryption != "" {
		if uploader.SSECustomerKey != "" {
			uploadInput.SSECustomerAlgorithm = aws.String(uploader.serverSideEncryption)
			uploadInput.SSECustomerKey = aws.String(sseCustomerKeyB64(uploader.SSECustomerKey))
			customerKeyMD5 := GetSSECustomerKeyMD5(uploader.SSECustomerKey)
			uploadInput.SSECustomerKeyMD5 = aws.String(customerKeyMD5)
		} else {
			uploadInput.ServerSideEncryption = types.ServerSideEncryption(uploader.serverSideEncryption)
		}

		if uploader.SSEKMSKeyID != "" {
			// Only aws:kms implies sseKmsKeyId, checked during validation
			uploadInput.SSEKMSKeyId = aws.String(uploader.SSEKMSKeyID)
		}
	}

	return uploadInput
}

func (uploader *Uploader) upload(ctx context.Context, bucket, path string, content io.Reader) error {
	input := uploader.createUploadInput(bucket, path, content)
	if uploader.verifyUploadOnNoSuchUpload {
		// S3 publishes the metadata atomically with the completed object. A fresh
		// token distinguishes this upload from an old object at the same key.
		attempt, err := uuid.NewRandom()
		if err != nil {
			return errors.Wrap(err, "generate S3 upload attempt ID")
		}
		input.Metadata = map[string]string{uploadAttemptMetadataKey: attempt.String()}
	}
	var options []func(*manager.Uploader)
	if size, known := uploadSize(content); uploader.autoPartSize && known {
		partSize := max((size+int64(manager.MaxUploadParts)-1)/int64(manager.MaxUploadParts), uploader.partSize)
		options = append(options, func(o *manager.Uploader) { o.PartSize = partSize })
	}
	_, err := uploader.uploaderAPI.Upload(ctx, input, options...)
	if err != nil && uploader.verifyUploadOnNoSuchUpload && isMissingCompletedUpload(err) {
		err = uploader.verifyCompletedUpload(ctx, input, err)
	}
	return errors.Wrapf(err, "failed to upload '%s' to bucket '%s'", path, bucket)
}

// Only reconcile an absent multipart session at completion, never a missing
// part/session while UploadPart is still running, or an unrelated HTTP 404.
func isMissingCompletedUpload(err error) bool {
	var operation *smithy.OperationError
	var apiErr smithy.APIError
	var response interface{ HTTPStatusCode() int }
	return errors.As(err, &operation) && operation.Operation() == "CompleteMultipartUpload" &&
		errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" &&
		errors.As(err, &response) && response.HTTPStatusCode() == 404
}

func (uploader *Uploader) verifyCompletedUpload(ctx context.Context, input *s3.PutObjectInput, uploadErr error) error {
	if uploader.headAPI == nil {
		return uploadErr
	}
	output, err := uploader.headAPI.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: input.Bucket, Key: input.Key,
		SSECustomerAlgorithm: input.SSECustomerAlgorithm,
		SSECustomerKey:       input.SSECustomerKey, SSECustomerKeyMD5: input.SSECustomerKeyMD5,
	})
	if err != nil {
		return errors.Wrapf(uploadErr, "could not verify completed upload with HeadObject: %v", err)
	}
	attempt := input.Metadata[uploadAttemptMetadataKey]
	if attempt == "" || output == nil || output.Metadata[uploadAttemptMetadataKey] != attempt {
		return errors.Wrap(uploadErr, "HeadObject did not confirm this upload attempt")
	}
	tracelog.WarningLogger.Printf("Confirmed completed S3 upload of %q with HeadObject after NoSuchUpload", aws.ToString(input.Key))
	return nil
}

// uploadSize obtains the remaining content size without reading or buffering
// it. Storage-to-storage copies provide SizedReader;
func uploadSize(content io.Reader) (int64, bool) {
	if sized, ok := content.(storage.SizedReader); ok {
		size := sized.RemainingSize()
		return size, size >= 0
	}
	return 0, false
}

// CreateUploaderAPI returns an uploader with customizable concurrency
// and part size. v2's manager.NewUploader requires the concrete S3 client (not
// our narrow API), since multipart upload uses methods beyond folder.go's set.
func CreateUploaderAPI(svc *s3.Client, partsize, concurrency int) UploaderAPI {
	return manager.NewUploader(svc, func(u *manager.Uploader) {
		u.PartSize = int64(partsize)
		u.Concurrency = concurrency
		u.RequestChecksumCalculation = svc.Options().RequestChecksumCalculation
	})
}

func partitionObjects(objects []storage.Object, blockSize int) [][]storage.Object {
	if blockSize <= 0 {
		return [][]storage.Object{objects}
	}
	partition := make([][]storage.Object, 0)
	for i := 0; i < len(objects); i += blockSize {
		if i+blockSize > len(objects) {
			partition = append(partition, objects[i:])
		} else {
			partition = append(partition, objects[i:i+blockSize])
		}
	}
	return partition
}
