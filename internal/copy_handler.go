package internal

import (
	"path"

	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
)

// HandleCopy copy backups from current storage to another
func HandleCopy(fromConfigFile string, toConfigFile string) {
	var fromFolder, fromError = configureFolderFromConfig(fromConfigFile)
	var toFolder, toError = configureFolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	copyObjects(fromFolder, toFolder)
}

func copyObjects(from storage.Folder, to storage.Folder) {
	objects, subfolders, _ := from.ListFolder()
	for _, object := range objects {
		tracelog.InfoLogger.Printf("Copy %s from %s to %s ", object.GetName(), from.GetPath(), to.GetPath())
		var readCloser, _ = from.ReadObject(object.GetName())
		to.PutObject(path.Join(from.GetPath(), object.GetName()), readCloser)
	}
	for _, subfolder := range subfolders {
		copyObjects(subfolder, to)
	}
}

func configureFolderFromConfig(configFile string) (storage.Folder, error) {
	var config = viper.New()
	SetDefaultValues(config)
	ReadConfigFromFile(config, configFile)
	CheckAllowedSettings(config)

	var folder, err = ConfigureFolderForSpecificConfig(config)
	if err != nil {
		tracelog.ErrorLogger.Println("Failed configure folder according to config " + configFile)
		tracelog.ErrorLogger.FatalError(err)
	}
	return folder, err
}
