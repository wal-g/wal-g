package mysql

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

// HandleCopyBackup copy specific backups from one storage to another
func HandleCopyBackup(fromConfigFile, toConfigFile, backupName, prefix string) {
	var from, fromError = internal.StorageFromConfig(fromConfigFile)
	var to, toError = internal.StorageFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := backupCopyingInfo(backupName, prefix, from.RootFolder(), to.RootFolder())
	logging.FatalOnError(err)

	slog.Debug(fmt.Sprintf("copying files %s\n", strings.Join(func()) []string {
		ret := make([]string, 0)
		for _, e := range infos {
			ret = append(ret, e.SrcObj.GetName())
		}

		return ret
	}(), ","))

	logging.FatalOnError(copy.Infos(infos))

	slog.Info(fmt.Sprintf("Success copyed backup %s.\n", backupName))
}

// HandleCopyBackup copy  all backups from one storage to another
func HandleCopyAll(fromConfigFile string, toConfigFile string) {
	var from, fromError = internal.StorageFromConfig(fromConfigFile)
	var to, toError = internal.StorageFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := WildcardInfo(from.RootFolder(), to.RootFolder())
	logging.FatalOnError(err)
	err = copy.Infos(infos)
	logging.FatalOnError(err)
	slog.Info(fmt.Sprintf("Success copyed all backups\n"))
}

func backupCopyingInfo(backupName, prefix string, from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	slog.Info(fmt.Sprintf("Handle backupname '%s'.", backupName))
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}
	slog.Info("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)

	objects, err := storage.ListFolderRecursively(from)
	if err != nil {
		return nil, err
	}

	var hasBackupPrefix = func(object storage.Object) bool { return strings.HasPrefix(object.GetName(), backupPrefix) }
	return copy.BuildCopyingInfos(
		from,
		to,
		objects,
		hasBackupPrefix,
		func(object storage.Object) string {
			return strings.Replace(object.GetName(), backup.Name, prefix+backup.Name, 1)
		},
		copy.NoopSourceTransformer,
	), nil
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
