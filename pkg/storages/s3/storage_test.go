package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestS3PartSizeModeConfiguration(t *testing.T) {
	baseSettings := map[string]string{
		endpointSetting:          "HTTP://s3.example.test/",
		skipValidationSetting:    "true",
		uploadConcurrencySetting: "1",
	}

	automaticStorage, err := ConfigureStorage(t.Context(), "s3://bucket/prefix", baseSettings)
	assert.NoError(t, err)
	automaticFolder := automaticStorage.RootFolder().(*Folder)
	assert.True(t, automaticFolder.uploader.autoPartSize)

	baseSettings[maxPartSizeSetting] = "33554432"
	explicitStorage, err := ConfigureStorage(t.Context(), "s3://bucket/prefix", baseSettings)
	assert.NoError(t, err)
	explicitFolder := explicitStorage.RootFolder().(*Folder)
	assert.False(t, explicitFolder.uploader.autoPartSize)
	assert.Equal(t, int64(33554432), explicitFolder.uploader.partSize)
}

func TestS3FolderCreatesWithoutAdditionalHeaders(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting:          "HTTP://s3.kek.lol.net/",
			skipValidationSetting:    "true",
			uploadConcurrencySetting: "1",
		})

	assert.NoError(t, err)
}

func TestS3FolderCreatesWithAdditionalHeadersJSON(t *testing.T) {
	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
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
	_, err := ConfigureStorage(t.Context(), waleS3Prefix,
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
	st, err := ConfigureStorage(t.Context(), waleS3Prefix,
		map[string]string{
			endpointSetting: "HTTP://s3.kek.lol.net/",
		})
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}
func TestS3FolderEndpointSource(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	waleS3Prefix := "s3://test-bucket/wal-g-test-folder/Sub0"
	st, err := ConfigureStorage(t.Context(), waleS3Prefix,
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
