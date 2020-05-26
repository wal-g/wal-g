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
	infos, err := getCopyingInfoToCopy(backupName, from, to, withoutHistory)
	tracelog.ErrorLogger.FatalOnError(err)
	isSuccess, err := StartCopy(infos)
	tracelog.ErrorLogger.FatalOnError(err)
	if isSuccess {
		tracelog.InfoLogger.Println("Success copy.")
	}
}

func StartCopy(infos []CopyingInfo) (bool, error) {
	var maxParallelJobsCount = 8 // TODO place for improvement
	for i := 0; i < len(infos); i += maxParallelJobsCount {
		errors := make(chan error)
		wgDone := make(chan bool)

		var lastIndex = utility.Min(i+maxParallelJobsCount, len(infos))
		var infosToCopy = infos[i:lastIndex]
		var wg sync.WaitGroup
		for _, info := range infosToCopy {
			wg.Add(1)
			go copyObject(info, &wg, errors)
		}
		wg.Wait()
		close(wgDone)

		select {
		case <-wgDone:
			break
		case err := <-errors:
			close(errors)
			return false, err
		}
	}
	return true, nil
}

func copyObject(info CopyingInfo, wg *sync.WaitGroup, errors chan error) {
	defer wg.Done()
	var objectName, from, to = info.Object.GetName(), info.From, info.To
	var readCloser, err = from.ReadObject(objectName)
	if err != nil {
		errors <- err
		return
	}
	var filename = path.Join(from.GetPath(), objectName)
	err = to.PutObject(filename, readCloser)
	if err != nil {
		errors <- err
		return
	}
	tracelog.InfoLogger.Printf("Copied '%s' from '%s' to '%s'.", objectName, from.GetPath(), to.GetPath())
}

func getCopyingInfoToCopy(backupName string, from storage.Folder, to storage.Folder, withoutHistory bool) ([]CopyingInfo, error) {
	if backupName == "" {
		tracelog.InfoLogger.Printf("Copy all backups and history.")
		return GetAllCopyingInfo(from, to)
	}
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	infos, err := GetBackupCopyingInfo(backup, from, to)
	if err != nil {
		return nil, err
	}
	if !withoutHistory {
		var history, err = GetHistoryCopyingInfo(backup, from, to)
		if err != nil {
			return nil, err
		}
		infos = append(infos, history...)
	}
	return infos, nil
}

func GetBackupCopyingInfo(backup *Backup, from storage.Folder, to storage.Folder) ([]CopyingInfo, error) {
	tracelog.InfoLogger.Print("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)
	var objects, err = storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}
	var hasBackupPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), backupPrefix) }
	return BuildCopyingInfos(from, to, objects, hasBackupPrefix), nil
}

func GetHistoryCopyingInfo(backup *Backup, from storage.Folder, to storage.Folder) ([]CopyingInfo, error) {
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

func GetAllCopyingInfo(from storage.Folder, to storage.Folder) ([]CopyingInfo, error) {
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
