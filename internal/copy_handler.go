package internal

import (
	"path"
	"strings"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type copyingInfo struct {
	object storage.Object
	from   storage.Folder
	to     storage.Folder
}

// HandleCopy copy specific or all backups from one storage to another
func HandleCopy(fromConfigFile string, toConfigFile string, backupName string, withoutHistory bool) {
	var from, fromError = ConfigureFolderFromConfig(fromConfigFile)
	var to, toError = ConfigureFolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	var infos, err = getObjectsToCopy(backupName, from, to, withoutHistory)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
		return
	}
	startCopy(infos)
}

func getObjectsToCopy(backupName string, from storage.Folder, to storage.Folder, withoutHistory bool) ([]copyingInfo, error) {
	if backupName == "" {
		tracelog.InfoLogger.Printf("Copy all backups and history.")
		return copyAll(from, to)
	}
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	infos, err := copyBackup(backup, from, to)
	if err != nil {
		return nil, err
	}
	if !withoutHistory {
		var history, err = copyHistory(backup, from, to)
		if err != nil {
			return nil, err
		}
		infos = append(infos, history...)
	}
	return infos, nil
}

func copyBackup(backup *Backup, from storage.Folder, to storage.Folder) ([]copyingInfo, error) {
	tracelog.InfoLogger.Print("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)
	var objects, err = storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}
	var hasBackupPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), backupPrefix) }
	return buildCopyingInfos(from, to, objects, hasBackupPrefix), nil
}

func copyHistory(backup *Backup, from storage.Folder, to storage.Folder) ([]copyingInfo, error) {
	tracelog.InfoLogger.Print("Collecting history files... ")
	var fromWalFolder = from.GetSubFolder(utility.WalPath)
	var lastWalFilename, err = getLastWalFilename(backup)
	if err != nil {
		return nil, err
	}
	tracelog.InfoLogger.Printf("after %s\n", lastWalFilename)
	objects, err := storage.ListFolderRecursively(fromWalFolder)
	if err != nil {
		return nil, err
	}
	var older = func(object storage.Object) bool { return lastWalFilename <= object.GetName() }
	return buildCopyingInfos(fromWalFolder, to, objects, older), nil
}

func copyAll(from storage.Folder, to storage.Folder) ([]copyingInfo, error) {
	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}
	return buildCopyingInfos(from, to, objects, func(object storage.Object) bool { return true }), nil
}

func copyObject(info copyingInfo, queue chan<- bool) {
	var objectName, from, to = info.object.GetName(), info.from, info.to
	var readCloser, err = from.ReadObject(objectName)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
		queue <- false
		return
	}
	var filename = path.Join(from.GetPath(), objectName)
	err = to.PutObject(filename, readCloser)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
		queue <- false
		return
	}
	tracelog.InfoLogger.Printf("Copied '%s' from '%s' to '%s'.", objectName, from.GetPath(), to.GetPath())
	queue <- true
}

func buildCopyingInfos(from storage.Folder, to storage.Folder,
	objects []storage.Object, condition func(storage.Object) bool) (infos []copyingInfo) {
	for _, object := range objects {
		if condition(object) {
			infos = append(infos, copyingInfo{object, from, to})
		}
	}
	return
}

func startCopy(infos []copyingInfo) {
	var maxParallelJobsCount = 8 // TODO place for improvement
	var jobs = make(chan bool, maxParallelJobsCount)

	for i := 0; i < len(infos); i += maxParallelJobsCount {
		var jobsToRun = utility.Min(maxParallelJobsCount, len(infos)-(i+1))
		for j := 0; j < jobsToRun; j++ {
			var info = infos[i+j]
			go copyObject(info, jobs)
		}
		for j := 0; j < jobsToRun; j++ {
			var isSuccess = <-jobs
			if !isSuccess {
				tracelog.DebugLogger.Fatal("Something went wrong.")
				return
			}
		}
	}
	tracelog.InfoLogger.Println("Success.")
}
