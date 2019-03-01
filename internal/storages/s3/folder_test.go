package s3

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"testing"
)

func TestS3Folder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	storageFolder, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting: "http://s3.mdst.yandex.net/",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
