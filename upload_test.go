package walg_test

import (
	"github.com/katie31/wal-g"
	"os"
	"testing"
)

/**
 *  Sets needed environment variables to empty strings.
 */
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

/**
 *  Sets needed environment variables.
 */
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

/**
 *  Tests that a valid S3 client is created and configured from
 *  environment variables.
 */
func TestConfigure(t *testing.T) {
	/***	Test empty environment variables	***/
	setEmpty(t)

	tu, pre, err := walg.Configure()

	err.Error()
	if _, ok := err.(*walg.UnsetEnvVarError); !ok {
		t.Errorf("upload: Expected error 'UnsetEnvVarError' but got %s", err)
	}

	if tu != nil || pre != nil {
		t.Errorf("upload: Expected empty uploader and prefix but got TU:%v and PREFIX:%v", tu, pre)
	}

	setFake(t)
	/***	Test invalid url	***/
	err = os.Setenv("WALE_S3_PREFIX", "test_fail:")
	if err != nil {
		t.Log(err)
	}

	_, _, err = walg.Configure()
	if err == nil {
		t.Errorf("upload: Expected to fail on fake url")
	}

	/***	Test created uploader and prefix 	***/
	err = os.Setenv("WALE_S3_PREFIX", "s3://bucket/server")
	if err != nil {
		t.Log(err)
	}
	tu, pre, err = walg.Configure()

	if *pre.Bucket != "bucket" {
		t.Errorf("upload: Prefix field 'Bucket' expected %s but got %s", "bucket", *pre.Bucket)
	}
	if *pre.Server != "server" {
		t.Errorf("upload: Prefix field 'Server' expected %s but got %s", "server", *pre.Server)
	}
	if tu == nil {
		t.Errorf("upload: did not create an uploader")
	}
	if err != nil {
		t.Errorf("upload: expected error to be '<nil>' but got %s", err)
	}

}

/**
 *  Tests that client is valid and created a new tar uploader.
 */
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

	tu := walg.NewTarUploader(mockClient, "bucket", "server", "region", 2, float64(1))
	tu.Upl = mockUploader

	maker := &walg.S3TarBallMaker{
		BaseDir:  "tmp",
		Trim:     "/usr/local",
		BkupName: "test",
		Tu:       tu,
	}

	tarBall := maker.Make()
	tarBall.SetUp()
	tarBall.Finish()
	if tu.Success == true {
		t.Errorf("upload: expected to fail to upload successfully")
	}

	tu.Upl = &mockS3Uploader{
		multierr: true,
	}

	tarBall = maker.Make()
	tarBall.SetUp()
	tarBall.Finish()
	if tu.Success == true {
		t.Errorf("upload: expected to fail to upload successfully")
	}

	_, err := tu.UploadWal("fake path")
	if err == nil {
		t.Errorf("upload: UploadWal expected error but got `<nil>`")
	}

}
