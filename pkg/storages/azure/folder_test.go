package azure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestAzureFolder(t *testing.T) {
	t.Skip("Credentials needed to run Azure Storage tests")

	st, err := ConfigureStorage("azure://test-container/test-folder/Sub0", make(map[string]string))
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}

func TestConfigureStorage_WithoutAccountNameSetting(t *testing.T) {
	settings := map[string]string{}
	prefix := "azure://test-container/test-folder/Sub0"

	_, err := ConfigureStorage(prefix, settings)

	assert.Error(t, err)
}

func TestConfigureStorage_WithValidInput(t *testing.T) {
	settings := map[string]string{
		"AZURE_STORAGE_ACCOUNT": "test-account",
	}
	prefix := "azure://test-container/test-folder/Sub0"

	storage, err := ConfigureStorage(prefix, settings)

	assert.NoError(t, err)
	assert.NotNil(t, storage)
}

var ConfigureAuthType = configureAuthType

func TestConfigureAccessKeyAuthType(t *testing.T) {
	settings := map[string]string{AccessKeySetting: "foo"}
	authType, accountToken, accessKey := ConfigureAuthType(settings)
	assert.Equal(t, authType, authTypeAccessKey)
	assert.Empty(t, accountToken)
	assert.Equal(t, accessKey, "foo")
}

func TestConfigureSASTokenAuth(t *testing.T) {
	settings := map[string]string{SASTokenSetting: "foo"}
	authType, accountToken, accessKey := ConfigureAuthType(settings)
	assert.Equal(t, authType, authTypeSASToken)
	assert.Equal(t, accountToken, "?foo")
	assert.Empty(t, accessKey)
}

func TestConfigureDefaultAuth(t *testing.T) {
	settings := make(map[string]string)
	authType, accountToken, accessKey := ConfigureAuthType(settings)
	assert.Empty(t, authType)
	assert.Empty(t, accountToken)
	assert.Empty(t, accessKey)
}
