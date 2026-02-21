package postgres

import (
	"fmt"
	"log/slog"
	"path"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopy copy specific or all backups from one storage to another
func HandleCopy(fromConfigFile string, toConfigFile string, backupName string, withAllHistory bool) {
	var from, fromError = internal.StorageFromConfig(fromConfigFile)
	var to, toError = internal.StorageFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := getCopyingInfos(backupName, from.RootFolder(), to.RootFolder(), withAllHistory)
	logging.FatalOnError(err)
	err = copy.Infos(infos)
	logging.FatalOnError(err)
	tracelog.InfoLogger.Println("Success copy.")
}

func BackupCopyingInfo(backup Backup, from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	slog.Info("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)

	var objects, err = storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	var hasBackupPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), backupPrefix) }
	return copy.BuildCopyingInfos(
		from,
		to,
		objects,
		hasBackupPrefix,
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	), nil
}

func getCopyingInfos(backupName string,
	from storage.Folder,
	to storage.Folder,
	withAllHistory bool) ([]copy.InfoProvider, error) {
	if backupName == "" {
		slog.Info(fmt.Sprintf("Copy all backups and history."))
		return WildcardInfo(from, to)
	}
	slog.Info(fmt.Sprintf("Handle backupname '%s'.", backupName))
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	pgBackup := ToPgBackup(backup)
	infos, err := BackupCopyingInfo(pgBackup, from, to)
	if err != nil {
		return nil, err
	}
	history, err := HistoryCopyingInfo(pgBackup, from, to, withAllHistory)
	if err != nil {
		return nil, err
	}
	infos = append(infos, history...)
	return infos, nil
}

func HistoryCopyingInfo(backup Backup, from storage.Folder, to storage.Folder, withAllHistory bool) ([]copy.InfoProvider, error) {
	slog.Debug("Collecting history files... ")

	var fromWalFolder = from.GetSubFolder(utility.WalPath)

	var lastWalFilename, err = GetLastWalFilename(backup)
	if err != nil {
		return make([]copy.InfoProvider, 0), err
	}

	firstWalFilename, err := GetFirstWalFilename(backup)
	if err != nil {
		return make([]copy.InfoProvider, 0), err
	}

	slog.Debug("getLastWalFilename not failed!")

	objects, err := storage.ListFolderRecursively(fromWalFolder)
	if err != nil {
		return nil, err
	}

	var match = func(object storage.Object) bool {
		return GetWalFileName(object.GetName()) >= firstWalFilename &&
			(withAllHistory || GetWalFileName(object.GetName()) <= lastWalFilename)
	}
	return copy.BuildCopyingInfos(
		fromWalFolder,
		to.GetSubFolder(utility.WalPath),
		objects,
		match,
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	), nil
}

func GetWalFileName(filename string) string {
	if !strings.Contains(filename, ".") {
		return filename
	}
	return strings.Split(filename, ".")[0]
}

func WildcardInfo(from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	return copy.BuildCopyingInfos(
		from,
		to,
		objects,
		func(object storage.Object) bool { return true },
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	), nil
}
