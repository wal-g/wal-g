package oss

import (
	"fmt"

	"github.com/alibabacloud-go/darabonba-openapi/v2/models"
	sts "github.com/alibabacloud-go/sts-20150401/v2/client"
	"github.com/alibabacloud-go/tea/dara"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscred "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.HashableStorage = &Storage{}

type Storage struct {
	rootFolder storage.Folder
	hash       string
}

type Config struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
	Region          string
	Bucket          string
	RootPath        string
	RoleARN         string
	RoleSessionName string
	SkipValidation  bool
	MaxRetries      int
}

func NewStorage(config *Config, rootWraps ...storage.WrapRootFolder) (*Storage, error) {
	client, err := configureClient(config)
	if err != nil {
		return nil, fmt.Errorf("configure client: %w", err)
	}

	var folder storage.Folder = NewFolder(client, config.Bucket, config.RootPath, config)

	for _, wrap := range rootWraps {
		folder = wrap(folder)
	}

	if !config.SkipValidation {
		err = folder.Validate()
		if err != nil {
			return nil, err
		}
	}

	hash, err := storage.ComputeConfigHash("oss", config)
	if err != nil {
		return nil, fmt.Errorf("compute config hash: %w", err)
	}

	return &Storage{folder, hash}, nil
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) ConfigHash() string {
	return s.hash
}

func (s *Storage) Close() error {
	// Nothing to close: the oss session doesn't require to be closed
	return nil
}

func configureClient(config *Config) (*oss.Client, error) {
	if config.Region == "" {
		return nil, fmt.Errorf("oss region is required")
	}

	accessKeyId := config.AccessKeyId
	accessKeySecret := config.AccessKeySecret
	securityToken := config.SecurityToken

	if config.RoleARN != "" {
		cred, err := credentials.NewCredential(&credentials.Config{
			Type:            oss.Ptr("access_key"),
			AccessKeyId:     oss.Ptr(accessKeyId),
			AccessKeySecret: oss.Ptr(accessKeySecret),
			SecurityToken:   oss.Ptr(securityToken),
		})
		if err != nil {
			return nil, fmt.Errorf("create credential: %w", err)
		}

		cfg := &models.Config{
			RegionId:   oss.Ptr(config.Region),
			Credential: cred,
		}

		stsClient, err := sts.NewClient(cfg)
		if err != nil {
			return nil, fmt.Errorf("create STS client: %w", err)
		}

		// Create an AssumeRole request.
		request := &sts.AssumeRoleRequest{
			RoleArn:         dara.String(config.RoleARN),
			RoleSessionName: dara.String(config.RoleSessionName),
		}

		// Initiate the request.
		response, err := stsClient.AssumeRole(request)
		if err != nil {
			return nil, fmt.Errorf("assume role: %w", err)
		}

		stsCreds := response.Body.Credentials
		if stsCreds == nil {
			return nil, fmt.Errorf("assume role response is missing credentials")
		}

		accessKeyId = *stsCreds.AccessKeyId
		accessKeySecret = *stsCreds.AccessKeySecret
		securityToken = *stsCreds.SecurityToken
	}

	credentialProvider := osscred.NewStaticCredentialsProvider(
		accessKeyId,
		accessKeySecret,
		securityToken,
	)

	ossConfig := oss.LoadDefaultConfig().
		WithRegion(config.Region).
		WithCredentialsProvider(credentialProvider)

	return oss.NewClient(ossConfig), nil
}
