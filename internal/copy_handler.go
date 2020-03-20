package internal

import (
	"path"
	"strconv"
	"strings"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type copyingStatus struct {
	objectName string
	fromPath   string
	toPath     string
	isSuccess  bool
}

// HandleCopy copy specific or all backups from one storage to another
func HandleCopy(fromConfigFile string, toConfigFile string, backupName string, withoutHistory bool) {
	var fromFolder, fromError = ConfigureFolderFromConfig(fromConfigFile)
	var toFolder, toError = ConfigureFolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}

	var queue = make(chan copyingStatus)
	if backupName == "" {
		tracelog.InfoLogger.Printf("Copy all backups and history.")
		copyAll(fromFolder, toFolder, queue)
	} else {
		tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
		backup, err := GetBackupByName(backupName, utility.BaseBackupPath, fromFolder)
		if err != nil {
			tracelog.ErrorLogger.FatalOnError(err)
			return
		}
		copyBackup(backup, fromFolder, toFolder, queue)
		if !withoutHistory {
			copyHistory(backup, fromFolder, toFolder, queue)
		}
	}
	handleQueue(queue)
}

func copyBackup(backup *Backup, from storage.Folder, to storage.Folder, queue chan copyingStatus) {
	tracelog.InfoLogger.Print("Copy base backup")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)
	var objects, err = storage.ListFolderRecursively(from)
	tracelog.DebugLogger.FatalOnError(err)
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), backupPrefix) {
			go copyObject(object, from, to, queue)
		}
	}
}

func copyHistory(backup *Backup, from storage.Folder, to storage.Folder, queue chan copyingStatus) {
	var fromWalFolder = from.GetSubFolder(utility.WalPath)
	var lastWalFilename, err = getLastWalFilename(backup)
	if err != nil {
		return
	}
	tracelog.InfoLogger.Printf("Copy all wal files after %s\n", lastWalFilename)
	objects, err := storage.ListFolderRecursively(fromWalFolder)
	if err != nil {
		tracelog.DebugLogger.FatalOnError(err)
		return
	}
	for _, object := range objects {
		if lastWalFilename <= object.GetName() {
			go copyObject(object, fromWalFolder, to, queue)
		}
	}
}

func copyAll(from storage.Folder, to storage.Folder, queue chan copyingStatus) {
	objects, err := storage.ListFolderRecursively(from)
	tracelog.DebugLogger.FatalOnError(err)
	for _, object := range objects {
		go copyObject(object, from, to, queue)
	}
}

func copyObject(object storage.Object, from storage.Folder, to storage.Folder, queue chan<- copyingStatus) {
	var readCloser, err = from.ReadObject(object.GetName())
	if err != nil {
		queue <- copyingStatus{object.GetName(), from.GetPath(), to.GetPath(), false}
		return
	}
	var filename = path.Join(from.GetPath(), object.GetName())
	err = to.PutObject(filename, readCloser)
	if err != nil {
		queue <- copyingStatus{object.GetName(), from.GetPath(), to.GetPath(), false}
		return
	}
	queue <- copyingStatus{object.GetName(), from.GetPath(), to.GetPath(), true}
}

func getLastWalFilename(backup *Backup) (string, error) {
	meta, err := backup.fetchMeta()
	if err != nil {
		tracelog.DebugLogger.FatalError(err)
		return "", err
	}
	timelineID64, err := strconv.ParseUint(backup.Name[len(utility.BackupNamePrefix):len(utility.BackupNamePrefix)+8], 0x10, sizeofInt32bits)
	if err != nil {
		tracelog.DebugLogger.FatalError(err)
		return "", err
	}
	timelineID := uint32(timelineID64)
	endWalSegmentNo := newWalSegmentNo(meta.FinishLsn - 1)
	return endWalSegmentNo.getFilename(timelineID), nil
}

func handleQueue(queue chan copyingStatus) {
	for status := range queue {
		if status.isSuccess {
			tracelog.InfoLogger.Printf("Copied '%s' from '%s' to '%s'.", status.objectName, status.fromPath, status.toPath)
		} else {
			tracelog.ErrorLogger.Fatalf("Coping '%s' from '%s' to '%s' failed.", status.objectName, status.fromPath, status.toPath)
		}
	}
}
