package walg

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"io"
)

// Archive contains information associated with
// a WAL archive.
type Archive struct {
	Prefix  *S3Prefix
	Archive *string
}

// CheckExistence checks that the specified WAL file exists.
func (archive *Archive) CheckExistence() (bool, error) {
	arch := &s3.HeadObjectInput{
		Bucket: archive.Prefix.Bucket,
		Key:    archive.Archive,
	}

	_, err := archive.Prefix.Svc.HeadObject(arch)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case NotFoundAWSErrorCode:
				return false, nil
			default:
				return false, awsErr
			}
		}
	}
	return true, nil
}

// GetETag aquires ETag of the object from S3
func (archive *Archive) GetETag() (*string, error) {
	arch := &s3.HeadObjectInput{
		Bucket: archive.Prefix.Bucket,
		Key:    archive.Archive,
	}

	h, err := archive.Prefix.Svc.HeadObject(arch)
	if err != nil {
		return nil, err
	}

	return h.ETag, nil
}

// GetArchive downloads the specified archive from S3.
func (archive *Archive) GetArchive() (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: archive.Prefix.Bucket,
		Key:    archive.Archive,
	}

	newArchive, err := archive.Prefix.Svc.GetObject(input)
	if err != nil {
		return nil, errors.Wrap(err, "GetArchive: s3.GetObject failed")
	}

	return newArchive.Body, nil
}
