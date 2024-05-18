package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestS3FolderCreatesWithoutAdditionalHeaders(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
		})

	assert.NoError(t, err)
}

func TestS3FolderCreatesWithAdditionalHeadersJSON(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:                 "HTTP://s3.kek.lol.net/",
			skipValidationSetting:           "true",
			uploadConcurrencySetting:        "1",
			requestAdditionalHeadersSetting: `{"X-Yandex-Prioritypass":"ok", "MyHeader":"32", "DROP_TABLE":"true"}`,
		})

	assert.NoError(t, err)
}

func TestS3FolderCreatesWithAdditionalHeadersYAML(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
			requestAdditionalHeadersSetting: `- X-Yandex-Prioritypass: "ok"
- MyHeader: "32"
- DROP_TABLE: "true"`,
		})

	assert.NoError(t, err)
}

func TestS3Folder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting: "HTTP://s3.kek.lol.net/",
		})
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}
func TestS3FolderEndpointSource(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			endpointSourceSetting:    "HTTP://localhost:80/",
			accessKeySetting:         "AKIAIOSFODNN7EXAMPLE",
			secretKeySetting:         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			uploadConcurrencySetting: "1",
			forcePathStyleSetting:    "True",
		})
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}
