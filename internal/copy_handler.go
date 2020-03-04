package internal

import (
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
)

// HandleCopy do things
func HandleCopy(folder *storage.Folder, toConfigFile string){
    var toStorageConfig = getConfig(toConfigFile)
    
}

func getConfig(configFile string) *viper.Viper {
	var config = viper.New()
	SetDefaultValues(config)
	ReadConfigFromFile(config, configFile)
	CheckAllowedSettings(config)

	return config;
}
