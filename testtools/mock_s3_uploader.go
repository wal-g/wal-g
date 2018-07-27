package testtools

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"io"
	"io/ioutil"
)

type mockMultiFailureError struct {
	s3manager.MultiUploadFailure
	err awserr.Error
}

func (m mockMultiFailureError) UploadID() string {
	return "mock ID"
}

func (m mockMultiFailureError) Error() string {
	return m.err.Error()
}

// Mock out uploader client for S3. Includes these methods:
// Upload(*UploadInput, ...func(*s3manager.Uploader))
type mockS3Uploader struct {
	s3manageriface.UploaderAPI
	multiErr bool
	err      bool
}

func NewMockS3Uploader(multiErr, err bool) *mockS3Uploader {
	return &mockS3Uploader{multiErr: multiErr, err: err}
}

func (u *mockS3Uploader) Upload(input *s3manager.UploadInput, f ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if u.err {
		return nil, awserr.New("UploadFailed", "mock Upload error", nil)
	}

	if u.multiErr {
		e := mockMultiFailureError{
			err: awserr.New("UploadFailed", "multiupload failure error", nil),
		}
		return nil, e
	}

	output := &s3manager.UploadOutput{
		Location:  *input.Bucket,
		VersionID: input.Key,
	}

	// Discard bytes to unblock pipe.
	_, err := io.Copy(ioutil.Discard, input.Body)
	if err != nil {
		return nil, err
	}

	return output, nil
}
