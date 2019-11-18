package internal_test

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"strings"
	"testing"
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
	viper.Reset()
	uploader, err := internal.ConfigureUploader(true)
	if _, ok := (errors.Cause(err)).(internal.UnconfiguredStorageError); !ok {
		t.Errorf("upload: Expected error 'UnconfiguredStorageError' but got %s", err)
	}
	assert.Nil(t, uploader)
	internal.InitConfig()
	internal.Configure()
	viper.Set("AWS_ACCESS_KEY_ID", "aws_access_key_id")
	viper.Set("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key")
	viper.Set("AWS_SESSION_TOKEN", "aws_session_token")
	viper.Set("WALE_S3_PREFIX", "gs://abc.com")
	viper.Set("AWS_ENDPOINT", "http://127.0.0.1:9000")
	viper.Set("AWS_REGION", "")
	_, err = internal.ConfigureUploader(true)
	assert.NoError(t, err)
	viper.Set("WALE_S3_PREFIX", "test_fail:")
	_, err = internal.ConfigureUploader(true)
	assert.Error(t, err)
	viper.Set("WALE_S3_PREFIX", bucketPath)
	uploader, err = internal.ConfigureUploader(true)
	assert.NoError(t, err)
	assert.Equal(t, expectedServer, strings.TrimSuffix(uploader.UploadingFolder.GetPath(), "/"))
	assert.NotNil(t, uploader)
	assert.NoError(t, err)
	// Test STANDARD_IA storage class
	viper.Set("WALG_S3_STORAGE_CLASS", "STANDARD_IA")
	_, err = internal.ConfigureUploader(true)
	assert.NoError(t, err)
}
