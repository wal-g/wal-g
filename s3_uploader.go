package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/pkg/errors"
	"io"
)

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
