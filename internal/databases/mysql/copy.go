package mysql

import (
	"context"
	"path"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopyBackup copy specific backups from one storage to another
func HandleCopyBackup(ctx context.Context, fromConfigFile, toConfigFile, backupName, prefix string) {
	var from, fromError = internal.StorageFromConfig(ctx, fromConfigFile)
	var to, toError = internal.StorageFromConfig(ctx, toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := backupCopyingInfo(ctx, backupName, prefix, from.RootFolder(), to.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Printf("copying files %s\n", strings.Join(func() []string {
		ret := make([]string, 0)
		for _, e := range infos {
			ret = append(ret, e.SrcObj.GetName())
		}

		return ret
	}(), ","))

	tracelog.ErrorLogger.FatalOnError(copy.Infos(ctx, infos))

	tracelog.InfoLogger.Printf("Success copyed backup %s.\n", backupName)
}

// HandleCopyBackup copy  all backups from one storage to another
func HandleCopyAll(ctx context.Context, fromConfigFile string, toConfigFile string) {
	var from, fromError = internal.StorageFromConfig(ctx, fromConfigFile)
	var to, toError = internal.StorageFromConfig(ctx, toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := WildcardInfo(ctx, from.RootFolder(), to.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)
	err = copy.Infos(ctx, infos)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Success copyed all backups\n")
}

func backupCopyingInfo(ctx context.Context, backupName, prefix string, from storage.Folder, to storage.Folder,
) ([]copy.InfoProvider, error) {
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := internal.GetBackupByName(ctx, backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}
	tracelog.InfoLogger.Print("Collecting backup files...")
	var backupPrefix = path.Join(utility.BaseBackupPath, backup.Name)

	objects, err := storage.ListFolderRecursively(ctx, from)
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

func WildcardInfo(ctx context.Context, from storage.Folder, to storage.Folder) ([]copy.InfoProvider, error) {
	objects, err := storage.ListFolderRecursively(ctx, from)
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
