package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"os"
	"testing"
)

func TestS3Folder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	os.Setenv("AWS_ENDPOINT", "http://s3.mdst.yandex.net/")
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	storageFolder, err := internal.ConfigureS3Folder(waleS3Prefix)

	assert.NoError(t, err)

	testStorageFolder(storageFolder, t)
}
