package multistorage

import (
	"fmt"
	"io"
	"sync"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func NewStorageFolderReader(mainFolder storage.Folder, failover map[string]storage.Folder) (internal.StorageFolderReader, error) {
	if len(failover) == 0 {
		return internal.NewFolderReader(mainFolder), nil
	}

	err := initStorageCache()
	if err != nil {
		return nil, err
	}

	failoverFolders := NewFailoverFolders(mainFolder, failover)

	folder, ok, err := FindCachedStorage(failoverFolders)
	if err != nil {
		return nil, err
	}

	if !ok {
		// if no cached, use default
		folder = NewDefaultFailoverFolder(mainFolder)
	}

	leftover := make([]FailoverFolder, 0)
	for i := range failoverFolders {
		if failoverFolders[i].Name != folder.Name {
			leftover = append(leftover, failoverFolders[i])
		}
	}

	return &StorageFolderReader{
		main:     folder,
		failover: leftover,
	}, nil
}

type StorageFolderReader struct {
	main FailoverFolder

	failover            []FailoverFolder
	aliveSearchComplete bool
	mu                  sync.Mutex
}

func (sfr *StorageFolderReader) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	// at first, try to read from the default storage,
	// if failed, check for any alive failover storages

	readCloser, originalErr := sfr.main.ReadObject(objectRelativePath)
	if originalErr == nil {
		return readCloser, nil
	}

	err := sfr.initFailoverStorages()
	if err != nil {
		tracelog.ErrorLogger.Printf("failover storages init failed: %v", err)
		return nil, originalErr
	}

	errors := []error{originalErr}
	for i := range sfr.failover {
		readCloser, err := sfr.failover[i].ReadObject(objectRelativePath)
		if err == nil {
			storageCache.Update(sfr.failover[i].Name)
			tracelog.WarningLogger.Printf("will read '%s' from failover storage '%s'",
				objectRelativePath, sfr.failover[i].Name)
			return readCloser, nil
		}
		errors = append(errors, err)

		tracelog.DebugLogger.Printf("failover storage '%s', reading object '%s': %v",
			sfr.failover[i].Name, objectRelativePath, err)
	}

	for i := range errors {
		if _, ok := errors[i].(storage.ObjectNotFoundError); !ok {
			// if we have at least one error that differs from the regular not found one
			// we must return a custom error since we can't say for sure if object exists or not
			return nil, fmt.Errorf("object %s unavailable, tried reading from %d storages: %v",
				objectRelativePath, len(errors), errors)
		}
	}

	return nil, storage.NewObjectNotFoundError(objectRelativePath)
}

func (sfr *StorageFolderReader) initFailoverStorages() error {
	sfr.mu.Lock()
	defer sfr.mu.Unlock()

	if sfr.aliveSearchComplete {
		return nil
	}

	aliveFolders, err := FindAliveStorages(sfr.failover, false)
	if err != nil {
		return err
	}

	sfr.failover = aliveFolders
	sfr.aliveSearchComplete = true
	return nil
}

func (sfr *StorageFolderReader) SubFolder(subFolderRelativePath string) internal.StorageFolderReader {
	sfr.mu.Lock()
	defer sfr.mu.Unlock()
	var failover []FailoverFolder

	for i := range sfr.failover {
		failover = append(failover, FailoverFolder{
			Folder: sfr.failover[i].GetSubFolder(subFolderRelativePath),
			Name:   sfr.failover[i].Name,
		})
	}

	return &StorageFolderReader{
		main:     FailoverFolder{sfr.main.GetSubFolder(subFolderRelativePath), sfr.main.Name},
		failover: failover,
	}
}
