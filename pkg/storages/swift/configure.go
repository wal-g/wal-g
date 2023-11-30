package swift

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	osUserNameSetting   = "OS_USERNAME"
	osPasswordSetting   = "OS_PASSWORD"
	osAuthURLSetting    = "OS_AUTH_URL"
	osTenantNameSetting = "OS_TENANT_NAME"
	osRegionNameSetting = "OS_REGION_NAME"
)

var SettingList = []string{
	osUserNameSetting,
	osPasswordSetting,
	osAuthURLSetting,
	osTenantNameSetting,
	osRegionNameSetting,
}

// TODO: Unit tests
func ConfigureStorage(
	prefix string,
	settings map[string]string,
	rootWraps ...storage.WrapRootFolder,
) (storage.HashableStorage, error) {
	container, rootPath, err := storage.GetPathFromPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("extract container and path from prefix %q: %w", prefix, err)
	}
	rootPath = storage.AddDelimiterToPath(rootPath)

	publicEnv := settings
	secretEnv := map[string]string{}
	if pass, ok := settings[osPasswordSetting]; ok {
		secretEnv[osPasswordSetting] = pass
		delete(publicEnv, osPasswordSetting)
	}

	config := &Config{
		Container:          container,
		RootPath:           rootPath,
		EnvVariables:       publicEnv,
		SecretEnvVariables: secretEnv,
	}

	st, err := NewStorage(config, rootWraps...)
	if err != nil {
		return nil, fmt.Errorf("create Swift storage: %w", err)
	}
	return st, nil
}
