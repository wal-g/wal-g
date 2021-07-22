package postgres

import (
	"path"
	"strings"

	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopy copy specific or all backups from one storage to another
func HandleCopy(fromConfigFile string, toConfigFile string, backupName string, withoutHistory bool) {
	var from, fromError = internal.FolderFromConfig(fromConfigFile)
	var to, toError = internal.FolderFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := getCopyingInfos(backupName, from, to, withoutHistory)
	tracelog.ErrorLogger.FatalOnError(err)
	err = copy.Infos(infos)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("Success copy.")
}

func BackupCopyingInfo(backup Backup, from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	tracelog.InfoLogger.Print("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)

	var objects, err = storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	var hasBackupPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), backupPrefix) }
	return copy.BuildCopyingInfos(from, to, objects, hasBackupPrefix, copy.NoopRenameFunc), nil
}

func getCopyingInfos(backupName string,
	from storage.Folder,
	to storage.Folder,
	withoutHistory bool) ([]copy.InfoProvider, error) {
	if backupName == "" {
		tracelog.InfoLogger.Printf("Copy all backups and history.")
		return WildcardInfo(from, to)
	}
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	pgBackup := ToPgBackup(backup)
	infos, err := BackupCopyingInfo(pgBackup, from, to)
	if err != nil {
		return nil, err
	}
	if !withoutHistory {
		var history, err = HistoryCopyingInfo(pgBackup, from, to)
		if err != nil {
			return nil, err
		}
		infos = append(infos, history...)
	}
	return infos, nil
}

func HistoryCopyingInfo(backup Backup, from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	tracelog.DebugLogger.Print("Collecting history files... ")

	var fromWalFolder = from.GetSubFolder(utility.WalPath)

	var lastWalFilename, err = GetLastWalFilename(backup)
	if err != nil {
		return make([]copy.InfoProvider, 0), nil
	}

	tracelog.DebugLogger.Print("getLastWalFilename not failed!")

	objects, err := storage.ListFolderRecursively(fromWalFolder)
	if err != nil {
		return nil, err
	}

	var older = func(object storage.Object) bool { return lastWalFilename <= object.GetName() }
	return copy.BuildCopyingInfos(fromWalFolder, to, objects, older, copy.NoopRenameFunc), nil
}

func WildcardInfo(from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	return copy.BuildCopyingInfos(from,
			to,
			objects,
			func(object storage.Object) bool { return true },
			copy.NoopRenameFunc),
		nil
}
