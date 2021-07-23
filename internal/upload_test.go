package internal_test

import (
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
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
	internal.InitConfig()
	internal.Configure()
	os.Setenv("AWS_ACCESS_KEY_ID", "aws_access_key_id")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key")
	os.Setenv("AWS_SESSION_TOKEN", "aws_session_token")
	os.Setenv("WALE_S3_PREFIX", "gs://abc.com")
	os.Setenv("AWS_ENDPOINT", "http://127.0.0.1:9000")
	os.Setenv("AWS_REGION", "")
	_, err = internal.ConfigureUploader()
	assert.NoError(t, err)
	os.Setenv("WALE_S3_PREFIX", "test_fail:")
	_, err = internal.ConfigureUploader()
	assert.Error(t, err)
	os.Setenv("WALE_S3_PREFIX", bucketPath)
	uploader, err = internal.ConfigureUploader()
	assert.NoError(t, err)
	assert.Equal(t, expectedServer, strings.TrimSuffix(uploader.UploadingFolder.GetPath(), "/"))
	assert.NotNil(t, uploader)
	assert.NoError(t, err)
	// Test STANDARD_IA storage class
	os.Setenv("WALG_S3_STORAGE_CLASS", "STANDARD_IA")
	_, err = internal.ConfigureUploader()
	assert.NoError(t, err)
}
