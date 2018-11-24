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
	Folder  *S3Folder
	Archive *string
}

// CheckExistence checks that the specified WAL file exists.
func (archive *Archive) CheckExistence() (bool, error) {
	arch := &s3.HeadObjectInput{
		Bucket: archive.Folder.Bucket,
		Key:    archive.Archive,
	}

	_, err := archive.Folder.S3API.HeadObject(arch)
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

func IsAwsNotExist(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == NotFoundAWSErrorCode || awsErr.Code() == NoSuchKeyAWSErrorCode {
			return true
		}
	}
	return false
}

// getETagAndReplicationStatue acquires ETag of the object from S3
func (archive *Archive) getETagAndReplicationStatue() (eTag *string ,replicationStatus *string, err error) {
	arch := &s3.HeadObjectInput{
		Bucket: archive.Folder.Bucket,
		Key:    archive.Archive,
	}

	h, err := archive.Folder.S3API.HeadObject(arch)
	if err != nil {
		return nil, nil,  err
	}

	return h.ETag, h.ReplicationStatus, nil
}

// GetArchive downloads the specified archive from S3.
func (archive *Archive) GetArchive() (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: archive.Folder.Bucket,
		Key:    archive.Archive,
	}

	newArchive, err := archive.Folder.S3API.GetObject(input)
	if err != nil {
		return nil, errors.Wrapf(err, "GetArchive: s3.GetObject failed getting '%v'", *archive.Archive)
	}

	return newArchive.Body, nil
}
