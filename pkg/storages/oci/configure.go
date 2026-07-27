package oci

import (
	"context"
	"fmt"
	"strings"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/pkg/storages/storage/setting"
)

const (
	regionSetting            = "OCI_REGION"
	tenancyOCIDSetting       = "OCI_TENANCY_OCID"
	securityTokenFileSetting = "OCI_SECURITY_TOKEN_FILE"
	privateKeyFileSetting    = "OCI_PRIVATE_KEY_FILE"
	configFileSetting        = "OCI_CONFIG_FILE"
	profileSetting           = "OCI_PROFILE"
	caCertFileSetting        = "WALG_OCI_CA_CERT_FILE"
	connectTimeoutSetting    = "OCI_CONNECT_TIMEOUT"
)

var SettingList = []string{
	regionSetting,
	tenancyOCIDSetting,
	securityTokenFileSetting,
	privateKeyFileSetting,
	configFileSetting,
	profileSetting,
	caCertFileSetting,
	connectTimeoutSetting,
}

const (
	defaultConnectTimeoutSeconds = 30
)

// ConfigureStorage creates OCI storage from configuration settings.
func ConfigureStorage(
	_ context.Context,
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	bucket, rootPath, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("extract bucket and path from prefix %q: %w", prefix, err)
	}

	connectTimeout, err := setting.Int64Optional(settings, connectTimeoutSetting, defaultConnectTimeoutSeconds)
	if err != nil {
		return nil, err
	}

	config := &Config{
		Region:            strings.TrimSpace(settings[regionSetting]),
		TenancyOCID:       strings.TrimSpace(settings[tenancyOCIDSetting]),
		SecurityTokenFile: strings.TrimSpace(settings[securityTokenFileSetting]),
		PrivateKeyFile:    strings.TrimSpace(settings[privateKeyFileSetting]),
		ConfigFile:        strings.TrimSpace(settings[configFileSetting]),
		Profile:           strings.TrimSpace(settings[profileSetting]),
		CACertFile:        strings.TrimSpace(settings[caCertFileSetting]),
		Bucket:            bucket,
		RootPath:          rootPath,
		ConnectTimeout:    connectTimeout,
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create OCI storage: %w", err)
	}
	return st, nil
}
