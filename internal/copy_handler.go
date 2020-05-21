package internal

import (
	"path"
	"strings"
	"sync"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type CopyingInfo struct {
	Object storage.Object
	From   storage.Folder
	To     storage.Folder
}

// HandleCopy copy specific or all backups from one storage to another
func HandleCopy(fromConfigFile string, toConfigFile string, backupName string, withoutHistory bool) {
	var from, fromError = ConfigureFolderFromConfig(fromConfigFile)
	var to, toError = ConfigureFolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	var infos, err = getObjectsToCopy(backupName, from, to, withoutHistory)
	tracelog.ErrorLogger.FatalOnError(err)
	StartCopy(infos)
}

func StartCopy(infos []CopyingInfo) {
	var maxParallelJobsCount = 8 // TODO place for improvement
	for i := 0; i < len(infos); i += maxParallelJobsCount {
		var lastIndex = utility.Min(i+maxParallelJobsCount, len(infos))
		var infosToCopy = infos[i:lastIndex]
		var wg sync.WaitGroup
		for _, info := range infosToCopy {
			wg.Add(1)
			go copyObject(info, &wg)
		}
		wg.Wait()
	}
	tracelog.InfoLogger.Println("Success.")
}

func copyObject(info CopyingInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	var objectName, from, to = info.Object.GetName(), info.From, info.To
	var readCloser, err = from.ReadObject(objectName)
	tracelog.ErrorLogger.FatalOnError(err)
	var filename = path.Join(from.GetPath(), objectName)
	err = to.PutObject(filename, readCloser)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Copied '%s' from '%s' to '%s'.", objectName, from.GetPath(), to.GetPath())
}

func getObjectsToCopy(backupName string, from storage.Folder, to storage.Folder, withoutHistory bool) ([]CopyingInfo, error) {
	if backupName == "" {
		tracelog.InfoLogger.Printf("Copy all backups and history.")
		return GetAllObjects(from, to)
	}
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	infos, err := GetBackupObjects(backup, from, to)
	if err != nil {
		return nil, err
	}
	if !withoutHistory {
		var history, err = GetHistoryObjects(backup, from, to)
		if err != nil {
			return nil, err
		}
		infos = append(infos, history...)
	}
	return infos, nil
}

func GetBackupObjects(backup *Backup, from storage.Folder, to storage.Folder) ([]CopyingInfo, error) {
	tracelog.InfoLogger.Print("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)
	var objects, err = storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}
	var hasBackupPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), backupPrefix) }
	return BuildCopyingInfos(from, to, objects, hasBackupPrefix), nil
}

func GetHistoryObjects(backup *Backup, from storage.Folder, to storage.Folder) ([]CopyingInfo, error) {
	tracelog.InfoLogger.Print("Collecting history files... ")
	var fromWalFolder = from.GetSubFolder(utility.WalPath)
	tracelog.InfoLogger.Print("getSubFolder succeess!")
	var lastWalFilename, err = getLastWalFilename(backup)
	if err != nil {
		return make([]CopyingInfo, 0), nil
	}
	tracelog.InfoLogger.Print("getLastWalFilename not failed!")
	tracelog.InfoLogger.Printf("after %s\n", lastWalFilename)
	objects, err := storage.ListFolderRecursively(fromWalFolder)
	if err != nil {
		return nil, err
	}
	var older = func(object storage.Object) bool { return lastWalFilename <= object.GetName() }
	return BuildCopyingInfos(fromWalFolder, to, objects, older), nil
}

func GetAllObjects(from storage.Folder, to storage.Folder) ([]CopyingInfo, error) {
	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}
	return BuildCopyingInfos(from, to, objects, func(object storage.Object) bool { return true }), nil
}

func BuildCopyingInfos(from storage.Folder, to storage.Folder, objects []storage.Object,
	condition func(storage.Object) bool) (infos []CopyingInfo) {
	for _, object := range objects {
		if condition(object) {
			infos = append(infos, CopyingInfo{object, from, to})
		}
	}
	return
}
