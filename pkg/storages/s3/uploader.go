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
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type UploaderConfig struct {
	UploadConcurrency            int
	MaxPartSize                  int
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
	return NewUploader(
		uploaderAPI,
		config.ServerSideEncryption,
		config.ServerSideEncryptionCustomer,
		config.ServerSideEncryptionKMSID,
		config.StorageClass,
		config.RetentionMode,
		config.RetentionPeriod,
	), nil
}

type Uploader struct {
	uploaderAPI          UploaderAPI
	serverSideEncryption string
	SSECustomerKey       string
	SSEKMSKeyID          string
	StorageClass         string
	RetentionMode        string
	RetentionPeriod      time.Duration
}

func NewUploader(uploaderAPI UploaderAPI, serverSideEncryption, sseCustomerKey, sseKmsKeyID, storageClass,
	retentionMode string, retentionPeriod int) *Uploader {
	if retentionMode == "" {
		retentionMode = "GOVERNANCE"
	}
	return &Uploader{uploaderAPI,
		serverSideEncryption,
		sseCustomerKey,
		sseKmsKeyID,
		storageClass,
		retentionMode,
		time.Duration(retentionPeriod)}
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
	_, err := uploader.uploaderAPI.Upload(ctx, input)
	return errors.Wrapf(err, "failed to upload '%s' to bucket '%s'", path, bucket)
}

// CreateUploaderAPI returns an uploader with customizable concurrency
// and part size. v2's manager.NewUploader requires the concrete S3 client (not
// our narrow API), since multipart upload uses methods beyond folder.go's set.
func CreateUploaderAPI(svc *s3.Client, partsize, concurrency int) UploaderAPI {
	return manager.NewUploader(svc, func(u *manager.Uploader) {
		u.PartSize = int64(partsize)
		u.Concurrency = concurrency
	})
}

func partitionStrings(strings []string, blockSize int) [][]string {
	// I've unsuccessfully tried this with interface{} but there was too much of casting
	if blockSize <= 0 {
		return [][]string{strings}
	}
	partition := make([][]string, 0)
	for i := 0; i < len(strings); i += blockSize {
		if i+blockSize > len(strings) {
			partition = append(partition, strings[i:])
		} else {
			partition = append(partition, strings[i:i+blockSize])
		}
	}
	return partition
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
