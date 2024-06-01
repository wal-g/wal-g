package azure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
    "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
    "github.com/golang/mock/gomock"
)

func TestAzureFolder(t *testing.T) {
	t.Skip("Credentials needed to run Azure Storage tests")

	storageFolder, err := ConfigureFolder("azure://test-container/test-folder/Sub0",
		make(map[string]string))

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}

var ConfigureAuthType = configureAuthType

func TestConfigureAccessKeyAuthType(t *testing.T) {
	settings := map[string]string{AccessKeySetting: "foo"}
	authType, accountToken, accessKey, clientID := ConfigureAuthType(settings)
	assert.Equal(t, authType, AzureAccessKeyAuth)
	assert.Empty(t, accountToken)
	assert.Equal(t, accessKey, "foo")
	assert.Empty(t, clientID)
}

func TestConfigureSASTokenAuth(t *testing.T) {
	settings := map[string]string{SasTokenSetting: "foo"}
	authType, accountToken, accessKey, clientID := ConfigureAuthType(settings)
	assert.Equal(t, authType, AzureSASTokenAuth)
	assert.Equal(t, accountToken, "?foo")
	assert.Empty(t, accessKey)
	assert.Empty(t, clientID)
}

func TestConfigureDefaultAuth(t *testing.T) {
	settings := make(map[string]string)
	authType, accountToken, accessKey, clientID := ConfigureAuthType(settings)
	assert.Empty(t, authType)
	assert.Empty(t, accountToken)
	assert.Empty(t, accessKey)
	assert.Empty(t, clientID)
}

func TestConfigureManagedIdentityAuth(t *testing.T) {
	settings := map[string]string{ClientIDSetting: "foo"}
	authType, accountToken, accessKey, clientID := ConfigureAuthType(settings)
	assert.Equal(t, authType, AzureManagedIdentityAuth)
	assert.Empty(t, accountToken)
	assert.Empty(t, accessKey)
	assert.Equal(t, clientID, "foo")
}
func TestGetContainerClientWithManagedIdentity(t *testing.T) {
	accountName := "test-account"
	storageEndpointSuffix := "test-endpoint"
	containerName := "test-container"
	timeout := time.Minute
	clientID := "test-client-id"

	containerClient, err := getContainerClientWithManagedIndetity(accountName, storageEndpointSuffix, containerName, timeout, clientID)
	assert.NoError(t, err)
	assert.NotNil(t, containerClient)
}

func TestGetContainerClientWithManagedIdentity2(t *testing.T) {
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    mockCred := azidentity.NewMockManagedIdentityCredential(ctrl)
    mockContainerClient := azblob.NewMockContainerClient(ctrl)

    accountName := "testAccount"
    storageEndpointSuffix := "core.windows.net"
    containerName := "testContainer"
    timeout := time.Second * 10
    clientID := "testClientID"

    mockCred.EXPECT().NewManagedIdentityCredential(gomock.Any()).Return(mockCred, nil)
    mockContainerClient.EXPECT().NewContainerClient(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockContainerClient, nil)

    containerClient, err := getContainerClientWithManagedIdentity(accountName, storageEndpointSuffix, containerName, timeout, clientID)
    if err != nil {
        t.Errorf("Unexpected error: %v", err)
    }
    if containerClient == nil {
        t.Error("Expected ContainerClient, got nil")
    }
}