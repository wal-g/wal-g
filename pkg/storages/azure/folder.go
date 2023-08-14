package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/pkg/errors"
)

const (
	AccountSetting    = "AZURE_STORAGE_ACCOUNT"
	AccessKeySetting  = "AZURE_STORAGE_ACCESS_KEY"
	SasTokenSetting   = "AZURE_STORAGE_SAS_TOKEN"
	EndpointSuffix    = "AZURE_ENDPOINT_SUFFIX"
	EnvironmentName   = "AZURE_ENVIRONMENT_NAME"
	BufferSizeSetting = "AZURE_BUFFER_SIZE"
	MaxBuffersSetting = "AZURE_MAX_BUFFERS"
	TryTimeoutSetting = "AZURE_TRY_TIMEOUT"
	minBufferSize     = 1024
	defaultBufferSize = 8 * 1024 * 1024
	minBuffers        = 1
	defaultBuffers    = 4
	defaultTryTimeout = 5
	defaultEnvName    = "AzurePublicCloud"
)

// nolint: revive
type AzureAuthType string

const (
	AzureAccessKeyAuth AzureAuthType = "AzureAccessKeyAuth"
	AzureSASTokenAuth  AzureAuthType = "AzureSASTokenAuth"
)

var SettingList = []string{
	AccountSetting,
	AccessKeySetting,
	SasTokenSetting,
	EnvironmentName,
	EndpointSuffix,
	BufferSizeSetting,
	MaxBuffersSetting,
}

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "Azure", format, args...)
}

func NewCredentialError(settingName string) storage.Error {
	return NewFolderError(errors.New("Credential error"),
		"%s setting is not set", settingName)
}

func NewFolder(
	uploadStreamOptions azblob.UploadStreamOptions,
	containerClient azblob.ContainerClient,
	credential *azblob.SharedKeyCredential,
	timeout time.Duration,
	path string) *Folder {
	return &Folder{
		uploadStreamOptions,
		containerClient,
		credential,
		timeout,
		path,
	}
}

func getContainerClientWithSASToken(
	accountName string,
	storageEndpointSuffix string,
	containerName string,
	timeout time.Duration,
	accountToken string) (*azblob.ContainerClient, error) {
	containerURLString := fmt.Sprintf("https://%s.blob.%s/%s%s", accountName, storageEndpointSuffix, containerName, accountToken)
	_, err := url.Parse(containerURLString)
	if err != nil {
		return nil, NewFolderError(err, "Unable to parse service URL with SAS token")
	}

	containerClient, err := azblob.NewContainerClientWithNoCredential(containerURLString, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: timeout},
	})
	return containerClient, err
}

func getContainerClientWithAccessKey(
	accountName string,
	storageEndpointSuffix string,
	containerName string,
	timeout time.Duration,
	credential *azblob.SharedKeyCredential) (*azblob.ContainerClient, error) {
	containerURLString := fmt.Sprintf("https://%s.blob.%s/%s", accountName, storageEndpointSuffix, containerName)
	_, err := url.Parse(containerURLString)
	if err != nil {
		return nil, NewFolderError(err, "Unable to parse service URL")
	}

	containerClient, err := azblob.NewContainerClientWithSharedKey(containerURLString, credential, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: timeout},
	})
	return containerClient, err
}

func getContainerClient(
	accountName string,
	storageEndpointSuffix string,
	containerName string,
	timeout time.Duration) (*azblob.ContainerClient, error) {
	defaultCredential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, NewFolderError(err, "Unable to construct default Azure credential chain")
	}

	containerURLString := fmt.Sprintf("https://%s.blob.%s/%s", accountName, storageEndpointSuffix, containerName)
	_, err = url.Parse(containerURLString)
	if err != nil {
		return nil, NewFolderError(err, "Unable to parse service URL")
	}

	containerClient, err := azblob.NewContainerClient(containerURLString, defaultCredential, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: timeout},
	})
	return containerClient, err
}

func configureAuthType(settings map[string]string) (AzureAuthType, string, string) {
	var ok bool
	var accountToken, accessKey string
	var authType AzureAuthType

	if accessKey, ok = settings[AccessKeySetting]; ok {
		authType = AzureAccessKeyAuth
	} else if accountToken, ok = settings[SasTokenSetting]; ok {
		authType = AzureSASTokenAuth
		// Tokens may or may not begin with ?, normalize these cases
		if !strings.HasPrefix(accountToken, "?") {
			accountToken = "?" + accountToken
		}
	}

	return authType, accountToken, accessKey
}

func ConfigureFolder(prefix string, settings map[string]string, methos ...string) (storage.Folder, error) {
	var accountName, storageEndpointSuffix string
	var ok bool
	if accountName, ok = settings[AccountSetting]; !ok {
		return nil, NewCredentialError(AccountSetting)
	}

	authType, accountToken, accountKey := configureAuthType(settings)

	var credential *azblob.SharedKeyCredential
	var err error
	if authType == AzureAccessKeyAuth {
		credential, err = azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, NewFolderError(err, "Unable to create credentials")
		}
	}

	var tryTimeout int
	if strTryTimeout, ok := settings[TryTimeoutSetting]; ok {
		tryTimeout, err = strconv.Atoi(strTryTimeout)
		if err != nil {
			return nil, NewFolderError(err, "Invalid azure try timeout setting")
		}
	} else {
		tryTimeout = defaultTryTimeout
	}
	timeout := time.Duration(tryTimeout) * time.Minute

	containerName, path, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, NewFolderError(err, "Unable to create container")
	}

	if storageEndpointSuffix, ok = settings[EndpointSuffix]; !ok {
		var environmentName string
		if environmentName, ok = settings[EnvironmentName]; !ok {
			environmentName = defaultEnvName
		}
		storageEndpointSuffix = getStorageEndpointSuffix(environmentName)
	}

	var containerClient *azblob.ContainerClient
	if authType == AzureSASTokenAuth {
		containerClient, err = getContainerClientWithSASToken(accountName, storageEndpointSuffix, containerName, timeout, accountToken)
	} else if authType == AzureAccessKeyAuth {
		containerClient, err = getContainerClientWithAccessKey(accountName, storageEndpointSuffix, containerName, timeout, credential)
	} else {
		// No explicitly configured auth method, try the default credential chain
		containerClient, err = getContainerClient(accountName, storageEndpointSuffix, containerName, timeout)
	}
	if err != nil {
		return nil, NewFolderError(err, "Unable to create service client")
	}
	path = storage.AddDelimiterToPath(path)
	return NewFolder(getUploadStreamOptions(settings), *containerClient, credential, timeout, path), nil
}

type Folder struct {
	uploadStreamOptions azblob.UploadStreamOptions
	containerClient     azblob.ContainerClient
	credential          *azblob.SharedKeyCredential
	timeout             time.Duration
	path                string
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	ctx := context.Background()
	blobClient, err := folder.containerClient.NewBlockBlobClient(path)
	if err != nil {
		return false, NewFolderError(err, "Unable to init Azure Blob client")
	}
	_, err = blobClient.GetProperties(ctx, nil)
	var stgErr *azblob.StorageError
	if err != nil && errors.As(err, &stgErr) && stgErr.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
		return false, nil
	}
	if err != nil {
		return false, NewFolderError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	blobPager := folder.containerClient.ListBlobsHierarchy("/", &azblob.ContainerListBlobsHierarchyOptions{Prefix: &folder.path})
	for blobPager.NextPage(context.Background()) {
		blobs := blobPager.PageResponse()
		//add blobs to the list of storage objects
		for _, blob := range blobs.Segment.BlobItems {
			objName := strings.TrimPrefix(*blob.Name, folder.path)
			updated := *blob.Properties.LastModified

			objects = append(objects, storage.NewLocalObject(objName, updated, *blob.Properties.ContentLength))
		}

		//Get subFolder names
		blobPrefixes := blobs.Segment.BlobPrefixes
		//add subFolders to the list of storage folders
		for _, blobPrefix := range blobPrefixes {
			subFolderPath := *blobPrefix.Name

			subFolders = append(subFolders, NewFolder(
				folder.uploadStreamOptions,
				folder.containerClient,
				folder.credential,
				folder.timeout,
				subFolderPath))
		}
	}
	err = blobPager.Err()
	if err != nil {
		return nil, nil, NewFolderError(err, "Unable to iterate %v", folder.path)
	}
	return objects, subFolders, err
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(
		folder.uploadStreamOptions,
		folder.containerClient,
		folder.credential,
		folder.timeout,
		storage.AddDelimiterToPath(storage.JoinPath(folder.path, subFolderRelativePath)))
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	blobClient, err := folder.containerClient.NewBlockBlobClient(path)
	if err != nil {
		return nil, NewFolderError(err, "Unable to init Azure Blob client")
	}

	get, err := blobClient.Download(context.Background(), nil)
	if err != nil {
		var storageError *azblob.StorageError
		errors.As(err, &storageError)
		if storageError.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
			return nil, storage.NewObjectNotFoundError(path)
		}
		return nil, NewFolderError(err, "Unable to download blob %s.", path)
	}
	reader := get.Body(nil)
	return reader, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	//Upload content to a block blob using full path
	path := storage.JoinPath(folder.path, name)
	blobClient, err := folder.containerClient.NewBlockBlobClient(path)
	if err != nil {
		return NewFolderError(err, "Unable to init Azure Blob client")
	}

	_, err = blobClient.UploadStream(context.Background(), content, folder.uploadStreamOptions)
	if err != nil {
		return NewFolderError(err, "Unable to upload blob %v", name)
	}

	tracelog.DebugLogger.Printf("Put %v done\n", name)
	return nil
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	var exists bool
	var err error
	if exists, err = folder.Exists(srcPath); !exists {
		if err == nil {
			return errors.New("object do not exists")
		}
		return err
	}
	var srcClient, dstClient *azblob.BlockBlobClient
	srcClient, err = folder.containerClient.NewBlockBlobClient(srcPath)
	if err != nil {
		return NewFolderError(err, "Unable to init Azure Blob client for copy source %s", srcPath)
	}
	dstClient, err = folder.containerClient.NewBlockBlobClient(dstPath)
	if err != nil {
		return NewFolderError(err, "Unable to init Azure Blob client for copy destination %s", dstPath)
	}
	_, err = dstClient.StartCopyFromURL(context.Background(), srcClient.URL(),
		&azblob.BlobStartCopyOptions{Tier: azblob.AccessTierHot.ToPtr()})
	return err
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		//Delete blob using blobClient obtained from full path to blob
		path := storage.JoinPath(folder.path, objectRelativePath)
		blobClient, err := folder.containerClient.NewBlockBlobClient(path)
		if err != nil {
			return NewFolderError(err, "Unable to init Azure Blob client")
		}
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		_, err = blobClient.Delete(context.Background(),
			&azblob.BlobDeleteOptions{DeleteSnapshots: azblob.DeleteSnapshotsOptionTypeInclude.ToPtr()})
		var stgErr *azblob.StorageError
		if err != nil && errors.As(err, &stgErr) && stgErr.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
			continue
		}
		if err != nil {
			return NewFolderError(err, "Unable to delete object %v", path)
		}
		//blob is deleted
	}
	return nil
}

func getUploadStreamOptions(settings map[string]string) azblob.UploadStreamOptions {
	// Configure the size of the rotating buffers
	bufSizeS := settings[BufferSizeSetting]
	bufferSize, err := strconv.Atoi(bufSizeS)
	if err != nil || bufferSize < minBufferSize {
		bufferSize = defaultBufferSize
	}
	// Configure the size of the rotating buffers and number of buffers
	maxBufS := settings[MaxBuffersSetting]
	maxBuffers, err := strconv.Atoi(maxBufS)
	if err != nil || maxBuffers < minBuffers {
		maxBuffers = defaultBuffers
	}
	return azblob.UploadStreamOptions{MaxBuffers: maxBuffers, BufferSize: bufferSize}
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
