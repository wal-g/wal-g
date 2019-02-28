package internal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
)

type SseKmsIdNotSetError struct {
	error
}

func NewSseKmsIdNotSetError() SseKmsIdNotSetError {
	return SseKmsIdNotSetError{errors.New("Configure: WALG_S3_SSE_KMS_ID must be set if using aws:kms encryption")}
}

func (err SseKmsIdNotSetError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type S3Uploader struct {
	uploaderAPI          s3manageriface.UploaderAPI
	serverSideEncryption string
	SSEKMSKeyId          string
	StorageClass         string
}

func NewS3Uploader(uploaderAPI s3manageriface.UploaderAPI, serverSideEncryption, sseKmsKeyId, storageClass string) *S3Uploader {
	return &S3Uploader{uploaderAPI, serverSideEncryption, sseKmsKeyId, storageClass}
}

// TODO : unit tests
func (uploader *S3Uploader) createUploadInput(bucket, path string, content io.Reader) *s3manager.UploadInput {
	uploadInput := &s3manager.UploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(path),
		Body:         content,
		StorageClass: aws.String(uploader.StorageClass),
	}

	if uploader.serverSideEncryption != "" {
		uploadInput.ServerSideEncryption = aws.String(uploader.serverSideEncryption)

		if uploader.SSEKMSKeyId != "" {
			// Only aws:kms implies sseKmsKeyId, checked during validation
			uploadInput.SSEKMSKeyId = aws.String(uploader.SSEKMSKeyId)
		}
	}

	return uploadInput
}

func (uploader *S3Uploader) upload(bucket, path string, content io.Reader) error {
	input := uploader.createUploadInput(bucket, path, content)
	_, err := uploader.uploaderAPI.Upload(input)
	return errors.Wrapf(err, "failed to upload '%s' to bucket '%s'", path, bucket)
}

// CreateUploaderAPI returns an uploader with customizable concurrency
// and part size.
func CreateUploaderAPI(svc s3iface.S3API, partsize, concurrency int) s3manageriface.UploaderAPI {
	uploaderAPI := s3manager.NewUploaderWithClient(svc, func(uploader *s3manager.Uploader) {
		uploader.PartSize = int64(partsize)
		uploader.Concurrency = concurrency
	})
	return uploaderAPI
}

// TODO : unit tests
func configureServerSideEncryption() (serverSideEncryption string, sseKmsKeyId string, err error) {
	serverSideEncryption, _ = LookupConfigValue("WALG_S3_SSE")
	sseKmsKeyId, _ = LookupConfigValue("WALG_S3_SSE_KMS_ID")

	// Only aws:kms implies sseKmsKeyId
	if (serverSideEncryption == "aws:kms") == (sseKmsKeyId == "") {
		return "", "", NewSseKmsIdNotSetError()
	}
	return
}

// TODO : unit tests
func configureS3Uploader(s3Client *s3.S3) (*S3Uploader, error) {
	var concurrency = getMaxUploadConcurrency(10)
	uploaderApi := CreateUploaderAPI(s3Client, DefaultStreamingPartSizeFor10Concurrency, concurrency)

	serverSideEncryption, sseKmsKeyId, err := configureServerSideEncryption()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure server side encryption")
	}

	storageClass, ok := LookupConfigValue("WALG_S3_STORAGE_CLASS")
	if !ok {
		storageClass = "STANDARD"
	}
	return NewS3Uploader(uploaderApi, serverSideEncryption, sseKmsKeyId, storageClass), nil
}
