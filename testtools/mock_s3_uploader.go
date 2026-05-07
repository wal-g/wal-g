package testtools

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	walgs3 "github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

// MockS3Uploader implements walgs3.UploaderAPI.
type MockS3Uploader struct {
	multiErr bool
	err      bool
	storage  *memory.KVS
}

var _ walgs3.UploaderAPI = (*MockS3Uploader)(nil)

func NewMockS3Uploader(multiErr, err bool, storage *memory.KVS) *MockS3Uploader {
	return &MockS3Uploader{multiErr: multiErr, err: err, storage: storage}
}

// mockMultiUploadFailure mimics manager.MultiUploadFailure for tests that branch
// on multipart-upload failure detection.
type mockMultiUploadFailure struct {
	err error
}

func (m mockMultiUploadFailure) Error() string  { return m.err.Error() }
func (m mockMultiUploadFailure) UploadID() string { return "mock ID" }

var _ manager.MultiUploadFailure = mockMultiUploadFailure{}

func (uploader *MockS3Uploader) Upload(_ context.Context, input *s3.PutObjectInput,
	_ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	if uploader.err {
		return nil, errors.New("mock Upload error")
	}

	if uploader.multiErr {
		return nil, mockMultiUploadFailure{err: errors.New("multiupload failure error")}
	}

	output := &manager.UploadOutput{
		Location:  *input.Bucket,
		VersionID: input.Key,
	}

	var err error
	if uploader.storage == nil {
		// Discard bytes to unblock pipe.
		_, err = io.Copy(io.Discard, input.Body)
	} else {
		var buf bytes.Buffer
		_, err = io.Copy(&buf, input.Body)
		uploader.storage.Store(*input.Bucket+*input.Key, buf)
	}
	if err != nil {
		return nil, err
	}

	return output, nil
}
