package s3_test

import (
	"errors"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/test/mocks"
	"go.uber.org/mock/gomock"
)

func TestS3FolderValidate_S3ReturnsErr(t *testing.T) {
	config := &s3.Config{
		Bucket:       "test",
		AccessKey:    "AKIAIOSFODNN7EXAMPLE",
		SessionToken: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Endpoint:     "HTTP://s3.kek.lol.net/",
		Region:       "region",
	}
	s3Client := mocks.NewMockAPI(gomock.NewController(t))
	s3Client.EXPECT().ListObjects(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock ListObjects error"))
	folder := s3.NewFolder(s3Client, nil, config.RootPath, config)
	err := folder.Validate(t.Context())
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
	s3Client := mocks.NewMockAPI(gomock.NewController(t))
	s3Client.EXPECT().ListObjects(gomock.Any(), gomock.Any()).Return(&awss3.ListObjectsOutput{}, nil)
	folder := s3.NewFolder(s3Client, nil, config.RootPath, config)
	err := folder.Validate(t.Context())
	assert.NoError(t, err)
}
