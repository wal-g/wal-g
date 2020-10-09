package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"path"
	"sync"
)

type Extractor interface {
	ExtractSubfolder() error
	GetBackupCopyingInfo(backup *Backup, from storage.Folder, to storage.Folder) ([]CopyingInfo, error)
}

type Copyer struct {
	e Extractor
}

type CopyingInfo struct {
	Object storage.Object
	From   storage.Folder
	To     storage.Folder
}

func New(e Extractor) Copyer {
	return Copyer{
		e: e,
	}
}

// HandleCopy copy specific or all backups from one storage to another
func (c* Copyer) HandleCopy(fromConfigFile string, toConfigFile string, backupName string, withoutHistory bool) {
	var from, fromError = ConfigureFolderFromConfig(fromConfigFile)
	var to, toError = ConfigureFolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := c.getCopyingInfo(backupName, from, to, withoutHistory)
	tracelog.ErrorLogger.FatalOnError(err)
	isSuccess, err := CopyInfos(infos)
	tracelog.ErrorLogger.FatalOnError(err)
	if isSuccess {
		tracelog.InfoLogger.Println("Success copy.")
	}
}

func CopyInfos(infos []CopyingInfo) (bool, error) {
	maxParallelJobsCount := 8

	tickets := make(chan interface{}, maxParallelJobsCount)

	for t := 0; t < maxParallelJobsCount; t++ {
		tickets <- nil
	}

	errors := make(chan error, maxParallelJobsCount * 2)
	var wg sync.WaitGroup

	for _, info := range infos {

		// do we have any errs yet?
		for len(errors) > 0 {
			if err := <-errors; err != nil {
				return false, err
			}
		}

		// block here
		_ = <-tickets
		wg.Add(1)

		go func(info CopyingInfo) {
			defer wg.Done()
			err := copyObject(info)
			tickets <- nil
			errors <- err
		}(info)
	}

	wg.Wait()

	for len(errors) > 0 {
		if err := <-errors; err != nil {
			return false, err
		}
	}

	return true, nil
}

func copyObject(info CopyingInfo) error {
	var objectName, from, to = info.Object.GetName(), info.From, info.To
	var readCloser, err = from.ReadObject(objectName)
	if err != nil {
		return err
	}
	var filename = path.Join(from.GetPath(), objectName)
	err = to.PutObject(filename, readCloser)
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Copied '%s' from '%s' to '%s'.", objectName, from.GetPath(), to.GetPath())
	return nil
}

func (c *Copyer) getCopyingInfo(backupName string, from storage.Folder, to storage.Folder, withoutHistory bool) ([]CopyingInfo, error) {
	if backupName == "" {
		tracelog.InfoLogger.Printf("Copy all backups and history.")
		return GetAllCopyingInfo(from, to)
	}
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	infos, err := c.e.GetBackupCopyingInfo(backup, from, to)
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
