package walg_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/katie31/wal-g"
	"os"
	"testing"
)

func (m *mockS3Client) GetBucketLocation(*s3.GetBucketLocationInput) (*s3.GetBucketLocationOutput, error) {
	mock := &s3.GetBucketLocationOutput{
		LocationConstraint: aws.String("mockBucketRegion"),
	}
	return mock, nil
}

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

	/***	Test fake credentials	***/
	setFake(t)

	tu, pre, err = walg.Configure()
	if err == nil {
		t.Errorf("upload: Expected to error on fake credentials but got '%v'", err)
	}

	/***	Test invalid url	***/
	err = os.Setenv("WALE_S3_PREFIX", "test_fail:")
	if err != nil {
		t.Log(err)
	}

	_, _, err = walg.Configure()
	if err == nil {
		t.Errorf("upload: Expected to fail on fake url")
	}

	/***	Test invalid config file	***/
	err = os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	if err != nil {
		t.Log(err)
	}

	_, _, err = walg.Configure()
	if err == nil {
		t.Errorf("upload: AWS_SDK_LOAD_CONFIG path is invalid")
	}

}

/**
 *  Tests that client is valid and created a new tar uploader.
 */
func TestValidUploader(t *testing.T) {
	mockSvc := &mockS3Client{}
	_, err := walg.Valid(mockSvc, "bucket")
	if err != nil {
		t.Errorf("upload: Mock S3 client should be valid but got '%s'", err)
	}

	tu := walg.NewTarUploader(mockSvc, "bucket", "server", "region")
	if tu == nil {
		t.Errorf("upload: Did not create a new tar uploader")
	}

	upl := walg.CreateUploader(mockSvc, 100, 3)
	if upl == nil {
		t.Errorf("upload: Did not create a new S3 UploadManager")
	}
}
