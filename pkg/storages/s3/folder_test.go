package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestS3FolderCreatesWithAdditionalHeadersJSON(t *testing.T) {
	assert := assert.New(t)

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting:          "HTTP://s3.kek.lol.net/",
			UploadConcurrencySetting: "1",
			RequestAdditionalHeaders: `{"method1":{"X-Yandex-Prioritypass":"ok", "MyHeader":"32", "DROP_TABLE":"true"}}`,
		})

	assert.NoError(err)
}

func TestS3FolderCreatesWithAdditionalHeadersJSONForMethod(t *testing.T) {
	assert := assert.New(t)

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting:          "HTTP://s3.kek.lol.net/",
			UploadConcurrencySetting: "1",
			RequestAdditionalHeaders: `{"method1":{"X-Yandex-Prioritypass":"ok", "MyHeader":"32", "DROP_TABLE":"true"}}`,
		}, "method1")

	assert.NoError(err)
}

func TestS3FolderCreatesWithAdditionalHeadersJSONForUnexistingMethod(t *testing.T) {
	assert := assert.New(t)

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting:          "HTTP://s3.kek.lol.net/",
			UploadConcurrencySetting: "1",
			RequestAdditionalHeaders: `{"method1":{"X-Yandex-Prioritypass":"ok", "MyHeader":"32", "DROP_TABLE":"true"}}`,
		}, "method2")

	assert.Error(err)
}

func TestS3FolderCreatesWithAdditionalHeadersYAML(t *testing.T) {
	assert := assert.New(t)

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting:          "HTTP://s3.kek.lol.net/",
			UploadConcurrencySetting: "1",
			RequestAdditionalHeaders: `method1:
- X-Yandex-Prioritypass: "ok"
- MyHeader: "32"
- DROP_TABLE: "true"`,
		})

	assert.NoError(err)
}

func TestS3FolderCreatesWithAdditionalHeadersYAMLForMethod(t *testing.T) {
	assert := assert.New(t)

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting:          "HTTP://s3.kek.lol.net/",
			UploadConcurrencySetting: "1",
			RequestAdditionalHeaders: `method1:
- X-Yandex-Prioritypass: "ok"
- MyHeader: "32"
- DROP_TABLE: "true"`,
		}, "method1")

	assert.NoError(err)
}

func TestS3FolderCreatesWithAdditionalHeadersYAMLForUnexistingMethod(t *testing.T) {
	assert := assert.New(t)

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting:          "HTTP://s3.kek.lol.net/",
			UploadConcurrencySetting: "1",
			RequestAdditionalHeaders: `method1:
- X-Yandex-Prioritypass: "ok"
- MyHeader: "32"
- DROP_TABLE: "true"`,
		}, "method2")

	assert.Error(err)
}

func TestS3Folder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	storageFolder, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting: "HTTP://s3.kek.lol.net/",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
func TestS3FolderEndpointSource(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	storageFolder, err := ConfigureFolder(waleS3Prefix,
		map[string]string{
			EndpointSetting:          "HTTP://s3.kek.lol.net/",
			EndpointSourceSetting:    "HTTP://localhost:80/",
			AccessKeySetting:         "AKIAIOSFODNN7EXAMPLE",
			SecretKeySetting:         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			UploadConcurrencySetting: "1",
			ForcePathStyleSetting:    "True",
		})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
