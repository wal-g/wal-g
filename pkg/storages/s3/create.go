package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func (folder *Folder) CreateObject(ctx context.Context, name string, content []byte) error {
	// Use a single conditional PUT, not a multipart upload or a HEAD/PUT pair.
	client, ok := folder.s3API.(interface {
		PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	})
	if !ok {
		return fmt.Errorf("S3 client does not support conditional PutObject")
	}
	input := folder.uploader.createUploadInput(*folder.bucket, folder.path+name, bytes.NewReader(content))
	input.IfNoneMatch = aws.String("*")
	_, err := client.PutObject(ctx, input)
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
		return fmt.Errorf("create %q: %w", name, storage.ErrObjectAlreadyExists)
	}
	if err != nil {
		return fmt.Errorf("conditionally create %q: %w", name, err)
	}
	return nil
}
