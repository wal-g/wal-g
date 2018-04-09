package walg_test

import (
	"os"
	"testing"

	"github.com/wal-g/wal-g"
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

	doConfigureWithBuсketPath(t, bucketPath, "server")
}

func TestConfigureBucketRoot(t *testing.T) {
	bucketPath := "s3://bucket/"

	doConfigureWithBuсketPath(t, bucketPath, "")
}

func TestConfigureBucketRoot2(t *testing.T) {
	bucketPath := "s3://bucket"

	doConfigureWithBuсketPath(t, bucketPath, "")
}

func TestConfigureDeepBucket(t *testing.T) {
	bucketPath := "s3://bucket/subdir/server"

	doConfigureWithBuсketPath(t, bucketPath, "subdir/server")
}

func doConfigureWithBuсketPath(t *testing.T, bucketPath string, expectedServer string) {
	//Test empty environment variables
	setEmpty(t)
	tu, pre, err := walg.Configure()
	if _, ok := err.(*walg.UnsetEnvVarError); !ok {
		t.Errorf("upload: Expected error 'UnsetEnvVarError' but got %s", err)
	}
	if tu != nil || pre != nil {
		t.Errorf("upload: Expected empty uploader and prefix but got TU:%v and PREFIX:%v", tu, pre)
	}
	setFake(t)
	//Test invalid url
	err = os.Setenv("WALE_S3_PREFIX", "test_fail:")
	if err != nil {
		t.Log(err)
	}
	_, _, err = walg.Configure()
	if err == nil {
		t.Errorf("upload: Expected to fail on fake url")
	}
	//Test created uploader and prefix
	err = os.Setenv("WALE_S3_PREFIX", bucketPath)
	if err != nil {
		t.Log(err)
	}
	tu, pre, err = walg.Configure()
	if err != nil {
		t.Errorf("upload: unexpected error %v", err)
	}
	if *pre.Bucket != "bucket" {
		t.Errorf("upload: Prefix field 'Bucket' expected %s but got %s", "bucket", *pre.Bucket)
	}
	if *pre.Server != expectedServer {
		t.Errorf("upload: Prefix field 'Server' expected %s but got %s", "server", *pre.Server)
	}
	if tu == nil {
		t.Errorf("upload: did not create an uploader")
	}
	if tu.StorageClass != "STANDARD" {
		t.Errorf("upload: TarUploader field 'StorageClass' expected %s but got %s", "STANDARD", tu.StorageClass)
	}
	if err != nil {
		t.Errorf("upload: expected error to be '<nil>' but got %s", err)
	}
	//Test STANDARD_IA storage class
	err = os.Setenv("WALG_S3_STORAGE_CLASS", "STANDARD_IA")
	defer os.Unsetenv("WALG_S3_STORAGE_CLASS")
	if err != nil {
		t.Log(err)
	}
	tu, pre, err = walg.Configure()
	if err != nil {
		t.Log(err)
	}
	if tu.StorageClass != "STANDARD_IA" {
		t.Errorf("upload: TarUploader field 'StorageClass' expected %s but got %s", "STANDARD_IA", tu.StorageClass)
	}
}

func TestValidUploader(t *testing.T) {
	mockSvc := &mockS3Client{}

	tu := walg.NewTarUploader(mockSvc, "bucket", "server", "region", 1, float64(1))
	if tu == nil {
		t.Errorf("upload: Did not create a new tar uploader")
	}

	upl := walg.CreateUploader(mockSvc, 100, 3)
	if upl == nil {
		t.Errorf("upload: Did not create a new S3 UploadManager")
	}
}

func TestUploadError(t *testing.T) {
	mockClient := &mockS3Client{}

	mockUploader := &mockS3Uploader{
		err: true,
	}

	tu := walg.NewTarUploader(mockClient, "bucket", "server", "region", 2, float64(2))
	tu.Upl = mockUploader

	maker := &walg.S3TarBallMaker{
		BaseDir:  "tmp",
		Trim:     "/usr/local",
		BkupName: "test",
		Tu:       tu,
	}

	tarBall := maker.Make()
	tarBall.SetUp(walg.MockArmedCrypter())

	tarBall.Finish(&walg.S3TarBallSentinelDto{})
	if tu.Success {
		t.Errorf("upload: expected to fail to upload successfully")
	}

	tu.Upl = &mockS3Uploader{
		multierr: true,
	}

	tarBall = maker.Make()
	tarBall.SetUp(walg.MockArmedCrypter())
	tarBall.Finish(&walg.S3TarBallSentinelDto{})
	if tu.Success {
		t.Errorf("upload: expected to fail to upload successfully")
	}

	_, err := tu.UploadWal("fake path")
	if err == nil {
		t.Errorf("upload: UploadWal expected error but got `<nil>`")
	}
}
