package mysql

import (
	"path"
	"strings"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopyBackup copy specific backups from one storage to another
func HandleCopyBackup(fromConfigFile, toConfigFile, backupName, prefix string, forceOverrite bool) {
	var from, fromError = internal.FolderFromConfig(fromConfigFile)
	var to, toError = internal.FolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := backupCopyingInfo(backupName, prefix, from, to, forceOverrite)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.ErrorLogger.FatalOnError(copy.Infos(infos))

	tracelog.InfoLogger.Printf("Success copyed backup %s.\n", backupName)
}

// HandleCopyBackup copy  all backups from one storage to another
func HandleCopyAll(fromConfigFile, toConfigFile string, forceOverrite bool) {
	var from, fromError = internal.FolderFromConfig(fromConfigFile)
	var to, toError = internal.FolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := WildcardInfo(from, to, forceOverrite)
	tracelog.ErrorLogger.FatalOnError(err)
	err = copy.Infos(infos)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Success copyed all backups\n")
}

func backupCopyingInfo(backupName, prefix string, from storage.Folder, to storage.Folder, forceOverrite bool) ([]copy.InfoProvider, error) {
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := internal.BackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}
	tracelog.InfoLogger.Print("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)

	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	var hasBackupPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), backupPrefix) }
	return copy.BuildCopyingInfos(from, to, objects, hasBackupPrefix, func(object storage.Object) string {
		return strings.Replace(object.GetName(), backup.Name, prefix+backup.Name, 1)
	}, forceOverrite), nil
}

func WildcardInfo(from storage.Folder, to storage.Folder, forceOverrite bool) ([]copy.InfoProvider, error) {
	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	return copy.BuildCopyingInfos(from, to, objects, func(object storage.Object) bool { return true }, copy.NoopRenameFunc, forceOverrite), nil
}
