package internal

import (
	"path"
	"strings"

	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopy copy backups from current storage to another
func HandleCopy(fromConfigFile string, toConfigFile string, backupName string) {
	var fromFolder, fromError = configureFolderFromConfig(fromConfigFile)
	var toFolder, toError = configureFolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}

	if backupName == "" {
		tracelog.InfoLogger.Printf("Copy all backups")
		copyAll(fromFolder, toFolder)
		return
	}

	tracelog.InfoLogger.Printf("Handle backupname '%s'", backupName)
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, fromFolder)
	if err != nil {
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}

	copyBaseBackup(backup, fromFolder, toFolder)
	copyWals(fromFolder, toFolder)
}

func copyBaseBackup(backup *Backup, from storage.Folder, to storage.Folder) {
	tracelog.InfoLogger.Print("Copy base backup")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)
	var objects, err = storage.ListFolderRecursively(from)
	tracelog.DebugLogger.FatalOnError(err)
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), backupPrefix) {
			copyObject(object, from, to)
		}
	}
}

func copyWals(from storage.Folder, to storage.Folder) {
	tracelog.InfoLogger.Print("Copy wal files")
	var walsFolder = from.GetSubFolder(utility.WalPath)
	copyAll(walsFolder, to)
}

func copyAll(from storage.Folder, to storage.Folder) {
	objects, err := storage.ListFolderRecursively(from)
	tracelog.DebugLogger.FatalOnError(err)
	for _, object := range objects {
		copyObject(object, from, to)
	}
}

func copyObject(object storage.Object, from storage.Folder, to storage.Folder) {
	tracelog.InfoLogger.Printf("Copy %s from %s to %s ", object.GetName(), from.GetPath(), to.GetPath())
	var readCloser, _ = from.ReadObject(object.GetName())
	var filename = path.Join(from.GetPath(), object.GetName())
	to.PutObject(filename, readCloser)
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
