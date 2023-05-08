package multistorage

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func NewUploader(baseUploader *internal.RegularUploader, failover map[string]storage.Folder) (internal.Uploader, error) {
	if len(failover) == 0 {
		return baseUploader, nil
	}

	failoverFolders := NewFailoverFolders(baseUploader.UploadingFolder, failover)

	err := initStorageCache()
	if err != nil {
		return nil, err
	}

	folder, ok, err := FindCachedStorage(failoverFolders)
	if err != nil {
		return nil, err
	}

	if !ok {
		tracelog.DebugLogger.Printf("Cached upload storage not found, will search for an available one")
		folder, err = chooseAliveUploadStorage(failoverFolders)
		if err != nil {
			return nil, err
		}
	}

	storageCache.Update(folder.Name)
	baseUploader.UploadingFolder = folder
	tracelog.DebugLogger.Printf("Active uploader is '%s'", folder.Name)
	return baseUploader, nil
}

func chooseAliveUploadStorage(storages []FailoverFolder) (FailoverFolder, error) {
	aliveFolders, err := FindAliveStorages(storages, true)
	if err != nil {
		return FailoverFolder{}, err
	}

	return aliveFolders[0], nil
}

func FindCachedStorage(storages []FailoverFolder) (FailoverFolder, bool, error) {
	if storageCache.IsActual() {
		ctx := storageCache.Copy()
		if ctx.LastGoodStorage != "" {
			for i := range storages {
				if storages[i].Name == ctx.LastGoodStorage {
					return storages[i], true, nil
				}
			}
		}
	}

	return FailoverFolder{}, false, nil
}
