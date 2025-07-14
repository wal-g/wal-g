package oss

import (
	"context"
	"fmt"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscred "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials/providers"
)

const (
	defaultSessionExpiration = 3600
)

func newCred(config *Config) (*cred, error) {
	var credentialsProvider providers.CredentialsProvider
	var err error

	if config.AccessKeyId == "" || config.AccessKeySecret == "" {
		return nil, fmt.Errorf("access key ID and secret are required")
	}

	if config.SecurityToken != "" {
		credentialsProvider, err = providers.NewStaticSTSCredentialsProviderBuilder().
			WithAccessKeyId(config.AccessKeyId).
			WithAccessKeySecret(config.AccessKeySecret).
			WithSecurityToken(config.SecurityToken).
			Build()
	} else {
		credentialsProvider, err = providers.NewStaticAKCredentialsProviderBuilder().
			WithAccessKeyId(config.AccessKeyId).
			WithAccessKeySecret(config.AccessKeySecret).
			Build()
	}
	if err != nil {
		return nil, fmt.Errorf("credentials provider: %w", err)
	}

	if config.RoleARN != "" {
		internalProvider := credentialsProvider
		credentialsProvider, err = providers.NewRAMRoleARNCredentialsProviderBuilder().
			WithCredentialsProvider(internalProvider).
			WithRoleArn(config.RoleARN).
			WithRoleSessionName(config.RoleSessionName).
			WithDurationSeconds(defaultSessionExpiration).
			Build()
		if err != nil {
			return nil, fmt.Errorf("ram role credential provider: %w", err)
		}
	}

	return &cred{
		provider: credentialsProvider,
	}, nil
}

type cred struct {
	provider providers.CredentialsProvider
}

func (c *cred) GetCredentials(ctx context.Context) (osscred.Credentials, error) {
	cc, err := c.provider.GetCredentials()
	if err != nil {
		return osscred.Credentials{}, err
	}

	return osscred.Credentials{
		AccessKeyID:     cc.AccessKeyId,
		AccessKeySecret: cc.AccessKeySecret,
		SecurityToken:   cc.SecurityToken,
	}, nil
}

func configureClient(config *Config) (*oss.Client, error) {
	if config.Region == "" {
		return nil, fmt.Errorf("oss region is required")
	}

	cred, err := newCred(config)
	if err != nil {
		return nil, fmt.Errorf("create credentials: %w", err)
	}

	ossConfig := oss.LoadDefaultConfig().
		WithRegion(config.Region).
		WithCredentialsProvider(cred).
		WithSignatureVersion(oss.SignatureVersionV4)

	return oss.NewClient(ossConfig), nil
}
