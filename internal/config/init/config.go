package init

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"

	"github.com/go-yaml/yaml"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/tracelog"
)

func init() {
	for _, adapter := range internal.StorageAdapters {
		allowedConfigKeys := []string{
			adapter.prefixName,
			config.ToWalgSettingName(adapter.prefixName),
		}
		config.UpdateAllowedConfig(allowedConfigKeys)
		for _, settingName := range adapter.settingNames {
			allowedConfigKeys = []string{
				"WALG_" + settingName,
				"WALE_" + settingName,
				settingName,
			}
			config.UpdateAllowedConfig(allowedConfigKeys)
		}
	}
	readConfig()
	verifyConfig()
}

func verifyConfig() {
	if WalgConfig == nil {
		return
	}
	for _, extension := range internal.Extensions {
		for key := range extension.GetAllowedConfigKeys() {
			config.UpdateAllowedConfig([]string{key})
		}
	}
	for k := range *WalgConfig {
		if _, ok := config.CheckAllowed(k); !ok {
			tracelog.ErrorLogger.Panic("Settings " + k + " is unknown")
		}
	}
}

func readConfig() {
	usr, err := user.Current()
	if err != nil {
		return
	}
	for _, unmarshal := range []func([]byte, interface{}) error{json.Unmarshal, yaml.Unmarshal} {
		cacheFilename := filepath.Join(usr.HomeDir, ".walg.json")
		file, err := ioutil.ReadFile(cacheFilename)
		// here we ignore whatever error can occur
		if err == nil {
			err = unmarshal(file, &WalgConfig)
			if err != nil {
				tracelog.ErrorLogger.Panic(err)
			}
			return
		} else if !os.IsNotExist(err) {
			tracelog.ErrorLogger.Panic(err)
		}
	}
}
