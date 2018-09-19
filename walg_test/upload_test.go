package walg_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
)

// Sets WAL-G needed environment variables to empty strings.
func setEmpty(t *testing.T) {
	err := os.Setenv("WALE_S3_PREFIX", "")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_REGION", "")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_ACCESS_KEY_ID", "")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_SECURITY_TOKEN", "")
	if err != nil {
		t.Log(err)
	}
}

// Sets fake environment variables.
func setFake(t *testing.T) {
	err := os.Setenv("WALE_S3_PREFIX", "wale_s3_prefix")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_REGION", "aws_region")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_ACCESS_KEY_ID", "aws_access_key_id")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key")
	if err != nil {
		t.Log(err)
	}
	err = os.Setenv("AWS_SECURITY_TOKEN", "aws_security_token")
	if err != nil {
		t.Log(err)
	}
}

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
	//Test empty environment variables
	setEmpty(t)
	uploader, folder, err := walg.Configure(false)
	if _, ok := err.(*walg.UnsetEnvVarError); !ok {
		t.Errorf("upload: Expected error 'UnsetEnvVarError' but got %s", err)
	}
	assert.Nil(t, uploader)
	assert.Nil(t, folder)
	setFake(t)
	//Test Minio
	err = os.Setenv("WALE_S3_PREFIX", "gs://abc.com")
	assert.NoError(t, err)
	err = os.Setenv("AWS_ENDPOINT", "http://127.0.0.1:9000")
	assert.NoError(t, err)
	err = os.Setenv("AWS_REGION", "")
	assert.NoError(t, err)
	_, _, err = walg.Configure(false)
	assert.NoError(t, err)
	//Test invalid url
	err = os.Setenv("WALE_S3_PREFIX", "test_fail:")
	assert.NoError(t, err)
	_, _, err = walg.Configure(false)
	assert.Error(t, err)
	//Test created uploader and prefix
	err = os.Setenv("WALE_S3_PREFIX", bucketPath)
	if err != nil {
		t.Log(err)
	}
	uploader, folder, err = walg.Configure(false)
	assert.NoError(t, err)
	assert.Equal(t, "bucket", *folder.Bucket)
	assert.Equal(t, expectedServer, folder.Server)
	assert.NotNil(t, uploader)
	assert.Equal(t, "STANDARD", uploader.StorageClass)
	assert.NoError(t, err)
	//Test STANDARD_IA storage class
	err = os.Setenv("WALG_S3_STORAGE_CLASS", "STANDARD_IA")
	defer os.Unsetenv("WALG_S3_STORAGE_CLASS")
	if err != nil {
		t.Log(err)
	}
	uploader, folder, err = walg.Configure(false)
	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "STANDARD_IA", uploader.StorageClass)
}

func TestValidUploader(t *testing.T) {
	mockSvc := testtools.NewMockS3Client(false, false)

	tu := testtools.NewMockUploader(false, false)
	assert.NotNil(t, tu)

	upl := walg.CreateUploader(mockSvc, 100, 3)
	assert.NotNil(t, upl)
}

func TestUploadError(t *testing.T) {
	uploader := testtools.NewMockUploader(false, true)

	maker := walg.NewS3TarBallMaker("test", uploader)

	tarBall := maker.Make(true)
	tarBall.SetUp(MockArmedCrypter())

	tarBall.Finish(&walg.S3TarBallSentinelDto{})
	assert.False(t, uploader.Success)

	uploader = testtools.NewMockUploader(true, false)

	tarBall = maker.Make(true)
	tarBall.SetUp(MockArmedCrypter())
	tarBall.Finish(&walg.S3TarBallSentinelDto{})
	assert.False(t, uploader.Success)
}
