package azure

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage/setting"
)

const (
	AccountSetting    = "AZURE_STORAGE_ACCOUNT"
	AccessKeySetting  = "AZURE_STORAGE_ACCESS_KEY"
	SASTokenSetting   = "AZURE_STORAGE_SAS_TOKEN"
	EndpointSuffix    = "AZURE_ENDPOINT_SUFFIX"
	EnvironmentName   = "AZURE_ENVIRONMENT_NAME"
	BufferSizeSetting = "AZURE_BUFFER_SIZE"
	BuffersSetting    = "AZURE_MAX_BUFFERS"
	TryTimeoutSetting = "AZURE_TRY_TIMEOUT"
)

// SettingList provides a list of GCS folder settings.
var SettingList = []string{
	AccountSetting,
	AccessKeySetting,
	SASTokenSetting,
	EnvironmentName,
	EndpointSuffix,
	BufferSizeSetting,
	BuffersSetting,
	TryTimeoutSetting,
}

const (
	minBufferSize     = 1024
	defaultBufferSize = 8 * 1024 * 1024
	minBuffers        = 1
	defaultBuffers    = 4
	defaultTryTimeout = 5
	defaultEnvName    = "AzurePublicCloud"
)

// TODO: Unit tests
func ConfigureStorage(
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	accountName, ok := settings[AccountSetting]
	if !ok {
		return nil, fmt.Errorf("%q is not specified", AccountSetting)
	}

	authType, sasToken, accessKey := configureAuthType(settings)

	tryTimeoutInt, err := setting.IntOptional(settings, TryTimeoutSetting, defaultTryTimeout)
	if err != nil {
		return nil, err
	}
	tryTimeout := time.Minute * time.Duration(tryTimeoutInt)

	containerName, path, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("extract container and path from prefix %q: %w", prefix, err)
	}
	path = storage.AddDelimiterToPath(path)

	var endpointSuffix string
	if endpointSuffix, ok = settings[EndpointSuffix]; !ok {
		var environmentName string
		if environmentName, ok = settings[EnvironmentName]; !ok {
			environmentName = defaultEnvName
		}
		endpointSuffix = getStorageEndpointSuffix(environmentName)
	}

	bufferSize, err := setting.IntOptional(settings, BufferSizeSetting, defaultBufferSize)
	if err != nil {
		return nil, err
	}
	if bufferSize < minBufferSize {
		bufferSize = minBufferSize
	}

	buffers, err := setting.IntOptional(settings, BuffersSetting, defaultBuffers)
	if err != nil {
		return nil, err
	}
	if buffers < minBuffers {
		buffers = minBuffers
	}

	config := &Config{
		Secrets: &Secrets{
			AccessKey: accessKey,
			SASToken:  sasToken,
		},
		RootPath:       path,
		Container:      containerName,
		AuthType:       authType,
		AccountName:    accountName,
		EndpointSuffix: endpointSuffix,
		TryTimeout:     tryTimeout,
		Uploader: &UploaderConfig{
			BufferSize: bufferSize,
			Buffers:    buffers,
		},
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create Google Cloud storage: %w", err)
	}
	return st, nil
}

func configureAuthType(settings map[string]string) (authType authType, token, key string) {
	var ok bool
	if key, ok = settings[AccessKeySetting]; ok {
		authType = authTypeAccessKey
	} else if token, ok = settings[SASTokenSetting]; ok {
		authType = authTypeSASToken
		// Tokens may or may not begin with ?, normalize these cases
		if !strings.HasPrefix(token, "?") {
			token = "?" + token
		}
	}

	return authType, token, key
}

// Function will get environment's name and return string with the environment's Azure storage account endpoint suffix.
// Expected names AzureUSGovernmentCloud, AzureChinaCloud, AzureGermanCloud. If any other name is used the func will return
// the Azure storage account endpoint suffix for AzurePublicCloud.
func getStorageEndpointSuffix(environmentName string) string {
	switch environmentName {
	case azure.USGovernmentCloud.Name:
		return azure.USGovernmentCloud.StorageEndpointSuffix
	case azure.ChinaCloud.Name:
		return azure.ChinaCloud.StorageEndpointSuffix
	case azure.GermanCloud.Name:
		return azure.GermanCloud.StorageEndpointSuffix
	default:
		return azure.PublicCloud.StorageEndpointSuffix
	}
}
