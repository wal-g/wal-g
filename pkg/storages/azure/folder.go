package azure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
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
	uploadStreamToBlockBlobOptions azblob.UploadStreamToBlockBlobOptions,
	containerClient azblob.ContainerClient,
	credential *azblob.SharedKeyCredential,
	timeout time.Duration,
	path string) *Folder {
	return &Folder{
		uploadStreamToBlockBlobOptions,
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
	containerUrlString := fmt.Sprintf("https://%s.blob.%s/%s%s", accountName, storageEndpointSuffix, containerName, accountToken)
	_, err := url.Parse(containerUrlString)
	if err != nil {
		return nil, NewFolderError(err, "Unable to parse service URL with SAS token")
	}

	containerClient, err := azblob.NewContainerClientWithNoCredential(containerUrlString, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: timeout},
	})
	return &containerClient, err
}

func getContainerClientWithAccessKey(
	accountName string,
	storageEndpointSuffix string,
	containerName string,
	timeout time.Duration,
	credential *azblob.SharedKeyCredential) (*azblob.ContainerClient, error) {
	containerUrlString := fmt.Sprintf("https://%s.blob.%s/%s", accountName, storageEndpointSuffix, containerName)
	_, err := url.Parse(containerUrlString)
	if err != nil {
		return nil, NewFolderError(err, "Unable to parse service URL")
	}

	containerClient, err := azblob.NewContainerClientWithSharedKey(containerUrlString, credential, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: timeout},
	})
	return &containerClient, err
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

	containerUrlString := fmt.Sprintf("https://%s.blob.%s/%s", accountName, storageEndpointSuffix, containerName)
	_, err = url.Parse(containerUrlString)
	if err != nil {
		return nil, NewFolderError(err, "Unable to parse service URL")
	}

	containerClient, err := azblob.NewContainerClient(containerUrlString, defaultCredential, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: timeout},
	})
	return &containerClient, err
}

func configureAuthType(settings map[string]string) (AzureAuthType, string, string) {
	var ok bool
	var accountToken, accessKey  string
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

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	var accountName, accountKey, accountToken, storageEndpointSuffix string
	var ok bool
	var authType AzureAuthType
	if accountName, ok = settings[AccountSetting]; !ok {
		return nil, NewCredentialError(AccountSetting)
	}

	authType, accountToken, accountKey = configureAuthType(settings)

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
	return NewFolder(getUploadStreamToBlockBlobOptions(settings), *containerClient, credential, timeout, path), nil
}

type Folder struct {
	uploadStreamToBlockBlobOptions azblob.UploadStreamToBlockBlobOptions
	containerClient                azblob.ContainerClient
	credential                     *azblob.SharedKeyCredential
	timeout                        time.Duration
	path                           string
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	ctx := context.Background()
	blobClient := folder.containerClient.NewBlockBlobClient(path)
	_, err := blobClient.GetProperties(ctx, &azblob.GetBlobPropertiesOptions{})
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
	blobPager := folder.containerClient.ListBlobsHierarchy("/", &azblob.ContainerListBlobHierarchySegmentOptions{Prefix: &folder.path})
	for blobPager.NextPage(context.Background()) {
		blobs := blobPager.PageResponse()
		//add blobs to the list of storage objects
		for _, blob := range blobs.Segment.BlobItems {
			objName := strings.TrimPrefix(*blob.Name, folder.path)
			updated := time.Time(*blob.Properties.LastModified)

			objects = append(objects, storage.NewLocalObject(objName, updated, *blob.Properties.ContentLength))
		}

		//Get subFolder names
		blobPrefixes := blobs.Segment.BlobPrefixes
		//add subFolders to the list of storage folders
		for _, blobPrefix := range blobPrefixes {
			subFolderPath := *blobPrefix.Name

			subFolders = append(subFolders, NewFolder(
				folder.uploadStreamToBlockBlobOptions,
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
	return
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(
		folder.uploadStreamToBlockBlobOptions,
		folder.containerClient,
		folder.credential,
		folder.timeout,
		storage.AddDelimiterToPath(storage.JoinPath(folder.path, subFolderRelativePath)))
}

// From https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/storage/azblob/zc_shared_policy_shared_key_credential.go
func buildStringToSign(c *azblob.SharedKeyCredential, req *http.Request) (string, error) {
	// https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services
	headers := req.Header
	contentLength := headers.Get("Content-Length")
	if contentLength == "0" {
		contentLength = ""
	}

	canonicalizedResource, err := buildCanonicalizedResource(c, req.URL)
	if err != nil {
		return "", err
	}

	stringToSign := strings.Join([]string{
		req.Method,
		headers.Get("Content-Encoding"),
		headers.Get("Content-Language"),
		contentLength,
		headers.Get("Content-MD5"),
		headers.Get("Content-Type"),
		"", // Empty date because x-ms-date is expected (as per web page above)
		headers.Get("If-Modified-Since"),
		headers.Get("If-Match"),
		headers.Get("If-None-Match"),
		headers.Get("If-Unmodified-Since"),
		headers.Get("Range"),
		buildCanonicalizedHeader(c, headers),
		canonicalizedResource,
	}, "\n")
	return stringToSign, nil
}

func buildCanonicalizedHeader(c *azblob.SharedKeyCredential, headers http.Header) string {
	cm := map[string][]string{}
	for k, v := range headers {
		headerName := strings.TrimSpace(strings.ToLower(k))
		if strings.HasPrefix(headerName, "x-ms-") {
			cm[headerName] = v // NOTE: the value must not have any whitespace around it.
		}
	}
	if len(cm) == 0 {
		return ""
	}

	keys := make([]string, 0, len(cm))
	for key := range cm {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ch := bytes.NewBufferString("")
	for i, key := range keys {
		if i > 0 {
			ch.WriteRune('\n')
		}
		ch.WriteString(key)
		ch.WriteRune(':')
		ch.WriteString(strings.Join(cm[key], ","))
	}
	return ch.String()
}

func buildCanonicalizedResource(c *azblob.SharedKeyCredential, u *url.URL) (string, error) {
	// https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services
	cr := bytes.NewBufferString("/")
	cr.WriteString(c.AccountName())

	if len(u.Path) > 0 {
		// Any portion of the CanonicalizedResource string that is derived from
		// the resource's URI should be encoded exactly as it is in the URI.
		// -- https://msdn.microsoft.com/en-gb/library/azure/dd179428.aspx
		cr.WriteString(u.EscapedPath())
	} else {
		// a slash is required to indicate the root path
		cr.WriteString("/")
	}

	// params is a map[string][]string; param name is key; params values is []string
	params, err := url.ParseQuery(u.RawQuery) // Returns URL decoded values
	if err != nil {
		return "", fmt.Errorf("failed to parse query params: %w", err)
	}

	if len(params) > 0 { // There is at least 1 query parameter
		var paramNames []string // We use this to sort the parameter key names
		for paramName := range params {
			paramNames = append(paramNames, paramName) // paramNames must be lowercase
		}
		sort.Strings(paramNames)

		for _, paramName := range paramNames {
			paramValues := params[paramName]
			sort.Strings(paramValues)

			// Join the sorted key values separated by ','
			// Then prepend "keyName:"; then add this string to the buffer
			cr.WriteString("\n" + paramName + ":" + strings.Join(paramValues, ","))
		}
	}
	return cr.String(), nil
}

// End from https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/storage/azblob/zc_shared_policy_shared_key_credential.go

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := storage.JoinPath(folder.path, objectRelativePath)
	blobClient := folder.containerClient.NewBlobClient(path)
	httpClient := &http.Client{Timeout: folder.timeout}

	req, err := http.NewRequest("GET", blobClient.URL(), nil)
	if err != nil {
		return nil, NewFolderError(err, "Unable to download blob %s.", path)
	}

	if folder.credential != nil {
		// Shared Key auth involves signing each request
		if d := req.Header.Get("x-ms-data"); d == "" {
			req.Header.Set("x-ms-date", time.Now().UTC().Format(http.TimeFormat))
		}
		stringToSign, stringErr := buildStringToSign(folder.credential, req)
		if stringErr != nil {
			return nil, NewFolderError(stringErr, "Unable to sign request to sign blob %s.", path)
		}
		signature, sigErr := folder.credential.ComputeHMACSHA256(stringToSign)
		if sigErr != nil {
			return nil, NewFolderError(sigErr, "Unable to sign request to sign blob %s.", path)
		}
		req.Header.Set("Authorization", strings.Join([]string{"SharedKey ", folder.credential.AccountName(), ":", signature}, ""))
	}

	resp, err := httpClient.Do(req)

	if err != nil {
		return nil, NewFolderError(err, "Unable to download blob %s.", path)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		resp.Body.Close()
		if resp.StatusCode == 404 {
			return nil, storage.NewObjectNotFoundError(path)
		} else {
			return nil, NewFolderError(errors.New(resp.Status), "Unable to download blob %s.", path)
		}
	}

	return resp.Body, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	//Upload content to a block blob using full path
	path := storage.JoinPath(folder.path, name)
	blobClient := folder.containerClient.NewBlockBlobClient(path)
	_, err := blobClient.UploadStreamToBlockBlob(context.Background(), content, folder.uploadStreamToBlockBlobOptions)
	if err != nil {
		return NewFolderError(err, "Unable to upload blob %v", name)
	}

	tracelog.DebugLogger.Printf("Put %v done\n", name)
	return nil
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return errors.New("object do not exists")
		} else {
			return err
		}
	}
	srcClient := folder.containerClient.NewBlockBlobClient(srcPath)
	dstClient := folder.containerClient.NewBlockBlobClient(dstPath)
	_, err := dstClient.StartCopyFromURL(context.Background(), srcClient.URL(), &azblob.StartCopyBlobOptions{Tier: azblob.AccessTierHot.ToPtr()})
	return err
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		//Delete blob using blobClient obtained from full path to blob
		path := storage.JoinPath(folder.path, objectRelativePath)
		blobClient := folder.containerClient.NewBlockBlobClient(path)
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		_, err := blobClient.Delete(context.Background(), &azblob.DeleteBlobOptions{DeleteSnapshots: azblob.DeleteSnapshotsOptionTypeInclude.ToPtr()})
		var stgErr *azblob.StorageError
		if err != nil && errors.As(err, &stgErr) && stgErr.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
			continue
		}
		if err != nil {
			return NewFolderError(err, "Unable to delete object %v", path)
		} else {
			//blob is deleted
		}
	}
	return nil
}

func getUploadStreamToBlockBlobOptions(settings map[string]string) azblob.UploadStreamToBlockBlobOptions {
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
	return azblob.UploadStreamToBlockBlobOptions{MaxBuffers: maxBuffers, BufferSize: bufferSize}
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
