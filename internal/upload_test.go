package internal_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/memory"

	"github.com/golang/mock/gomock"
	"github.com/wal-g/wal-g/test/mocks"
)

func TestConfigure(t *testing.T) {
	bucketPath := "s3://bucket/server"

	doConfigureWithBucketPath(t, bucketPath, "server")
}

func TestConfigureBucketRoot(t *testing.T) {
	bucketPath := "s3://bucket/"

	doConfigureWithBucketPath(t, bucketPath, "")
}

func TestConfigureBucketRoot2(t *testing.T) {
	bucketPath := "s3://bucket"

	doConfigureWithBucketPath(t, bucketPath, "")
}

func TestConfigureDeepBucket(t *testing.T) {
	bucketPath := "s3://bucket/subdir/server"

	doConfigureWithBucketPath(t, bucketPath, "subdir/server")
}

func doConfigureWithBucketPath(t *testing.T, bucketPath string, expectedServer string) {
	// Test empty environment variables
	os.Unsetenv("WALE_S3_PREFIX")
	uploader, err := internal.ConfigureUploader()
	if _, ok := (errors.Cause(err)).(internal.UnconfiguredStorageError); !ok {
		t.Errorf("upload: Expected error 'UnconfiguredStorageError' but got %s", err)
	}
	assert.Nil(t, uploader)
	internal.ConfigureSettings("")
	conf.InitConfig()
	conf.Configure()
	os.Setenv("AWS_ACCESS_KEY_ID", "aws_access_key_id")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key")
	os.Setenv("AWS_SESSION_TOKEN", "aws_session_token")
	os.Setenv("WALE_S3_PREFIX", "gs://abc.com")
	os.Setenv("AWS_ENDPOINT", "http://127.0.0.1:9000")
	os.Setenv("AWS_REGION", "")
	os.Setenv("S3_SKIP_VALIDATION", "true")
	_, err = internal.ConfigureUploader()
	assert.NoError(t, err)
	os.Setenv("WALE_S3_PREFIX", "test_fail:")
	_, err = internal.ConfigureUploader()
	assert.Error(t, err)
	os.Setenv("WALE_S3_PREFIX", bucketPath)
	uploader, err = internal.ConfigureUploader()
	assert.NoError(t, err)
	assert.Equal(t, expectedServer, strings.TrimSuffix(uploader.Folder().GetPath(), "/"))
	assert.NotNil(t, uploader)
	assert.NoError(t, err)
	// Test STANDARD_IA storage class
	os.Setenv("WALG_S3_STORAGE_CLASS", "STANDARD_IA")
	_, err = internal.ConfigureUploader()
	assert.NoError(t, err)
}

func TestUpload(t *testing.T) {
	reader := bytes.NewReader([]byte("some text"))
	compressor, errComp := internal.ConfigureCompressor()
	assert.NoError(t, errComp)
	kvs := memory.NewKVS()
	st := memory.NewStorage("gs://x4m-test/walg-bucket", kvs)
	folder := st.RootFolder()

	uploader := internal.NewRegularUploader(compressor, folder)

	err := uploader.Upload(context.Background(), "", reader)

	assert.NoError(t, err)

	_, objErr := uploader.UploadingFolder.ReadObject("")

	assert.NoError(t, objErr)
}

func TestUploadMock(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	folder := mocks.NewMockFolder(mockCtrl)
	defer mockCtrl.Finish()

	compressor, errComp := internal.ConfigureCompressor()
	assert.NoError(t, errComp)
	uploader := internal.NewRegularUploader(compressor, folder)

	reader := bytes.NewReader([]byte("some text"))

	folder.EXPECT().PutObjectWithContext(gomock.Any(), "some/path", gomock.Any()).Return(nil)
	folder.EXPECT().PutObjectWithContext(gomock.Any(), "path/to/incorrect/file", gomock.Any()).Return(errors.New("some error"))

	uploadWithoutErr := uploader.Upload(context.Background(), "some/path", reader)

	assert.NoError(t, uploadWithoutErr)

	uploadWithErr := uploader.Upload(context.Background(), "path/to/incorrect/file", reader)

	assert.Error(t, uploadWithErr)
}
