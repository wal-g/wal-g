package azure

import (
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	hash       string
}

type Config struct {
	Secrets        *Secrets `json:"-"`
	RootPath       string
	Container      string
	AuthType       authType
	AccountName    string
	EndpointSuffix string
	TryTimeout     time.Duration
	Uploader       *UploaderConfig
}

type Secrets struct {
	AccessKey string
	SASToken  string
}

type UploaderConfig struct {
	BufferSize int
	Buffers    int
}

type authType string

const (
	authTypeNotSpecified authType = ""
	authTypeAccessKey    authType = "AzureAccessKeyAuth"
	authTypeSASToken     authType = "AzureSASTokenAuth"
)

// TODO: Unit tests
func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	var containerClient *azblob.ContainerClient
	var err error
	switch config.AuthType {
	case authTypeSASToken:
		containerClient, err = containerClientWithSASToken(config)
	case authTypeAccessKey:
		containerClient, err = containerClientWithAccessKey(config)
	default:
		// If the auth method isn't specified, try the default credential chain
		containerClient, err = containerClientWithDefaultAuth(config)
	}
	if err != nil {
		return nil, fmt.Errorf("create Azure container client: %w", err)
	}

	uploadStreamOpts := azblob.UploadStreamOptions{
		BufferSize: config.Uploader.BufferSize,
		MaxBuffers: config.Uploader.Buffers,
	}

	var folder storage.Folder = NewFolder(config.RootPath, *containerClient, uploadStreamOpts, config.TryTimeout)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	hash, err := storage.ComputeConfigHash("azure", config)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{folder, hash}, nil
}

func containerClientWithSASToken(config *Config) (*azblob.ContainerClient, error) {
	containerURLString := fmt.Sprintf(
		"https://%s.blob.%s/%s%s",
		config.AccountName,
		config.EndpointSuffix,
		config.Container,
		config.Secrets.SASToken,
	)
	_, err := url.Parse(containerURLString)
	if err != nil {
		return nil, fmt.Errorf("parse service URL with SAS token: %w", err)
	}

	containerClient, err := azblob.NewContainerClientWithNoCredential(containerURLString, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: config.TryTimeout},
	})
	return containerClient, err
}

func containerClientWithAccessKey(config *Config) (*azblob.ContainerClient, error) {
	credential, err := azblob.NewSharedKeyCredential(config.AccountName, config.Secrets.AccessKey)
	if err != nil {
		return nil, fmt.Errorf("create shared key credentials: %w", err)
	}
	containerURLString := fmt.Sprintf(
		"https://%s.blob.%s/%s",
		config.AccountName,
		config.EndpointSuffix,
		config.Container,
	)
	_, err = url.Parse(containerURLString)
	if err != nil {
		return nil, fmt.Errorf("parse service URL: %w", err)
	}

	containerClient, err := azblob.NewContainerClientWithSharedKey(containerURLString, credential, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: config.TryTimeout},
	})
	return containerClient, err
}

func containerClientWithDefaultAuth(config *Config) (*azblob.ContainerClient, error) {
	defaultCredential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("construct the default Azure credential chain: %w", err)
	}

	containerURLString := fmt.Sprintf(
		"https://%s.blob.%s/%s",
		config.AccountName,
		config.EndpointSuffix,
		config.Container,
	)
	_, err = url.Parse(containerURLString)
	if err != nil {
		return nil, fmt.Errorf("parse service URL: %w", err)
	}

	containerClient, err := azblob.NewContainerClient(containerURLString, defaultCredential, &azblob.ClientOptions{
		Retry: policy.RetryOptions{TryTimeout: config.TryTimeout},
	})
	return containerClient, err
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) ConfigHash() string {
	return s.hash
}

func (s *Storage) Close() error {
	// Nothing to close: the Azure container client doesn't require to be closed
	return nil
}
