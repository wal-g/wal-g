package s3_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/testtools"
)

func TestS3FolderValidate_S3ReturnsErr(t *testing.T) {
	config := &s3.Config{
		Bucket:       "test",
		AccessKey:    "AKIAIOSFODNN7EXAMPLE",
		SessionToken: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Endpoint:     "HTTP://s3.kek.lol.net/",
		Region:       "region",
	}
	s3Client := testtools.NewMockS3Client(true, false)
	folder := s3.NewFolder(s3Client, nil, config.RootPath, config)
	err := folder.Validate()
	assert.Contains(t, err.Error(), "bad credentials")
}

func TestS3FolderValidate_S3DoesNotReturnErr(t *testing.T) {
	config := &s3.Config{
		Bucket:       "test",
		AccessKey:    "AKIAIOSFODNN7EXAMPLE",
		SessionToken: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Endpoint:     "HTTP://s3.kek.lol.net/",
		Region:       "region",
	}
	s3Client := testtools.NewMockS3Client(false, false)
	folder := s3.NewFolder(s3Client, nil, config.RootPath, config)
	err := folder.Validate()
	assert.NoError(t, err)
}
