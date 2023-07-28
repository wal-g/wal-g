package multistorage

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func executeOnAllStorages(fn func(folder storage.Folder) error) error {
	failover, err := internal.InitFailoverStorages()
	if err != nil {
		return err
	}

	folder, err := internal.ConfigureFolder()
	if err != nil {
		return err
	}
	toRun := NewFailoverFolders(folder, failover)

	atLeastOneOK := false
	for _, f := range toRun {
		tracelog.InfoLogger.Printf("storage %s", f.Name)
		err := fn(f)
		tracelog.ErrorLogger.PrintOnError(err)
		if err == nil {
			atLeastOneOK = true
		}
	}

	if !atLeastOneOK {
		return fmt.Errorf("all storages failed")
	}

	return nil
}

func ExecuteOnStorage(target string, fn func(folder storage.Folder) error) error {
	if target == "all" {
		return executeOnAllStorages(fn)
	}

	folder, err := ConfigureStorageFolder(target)
	if err != nil {
		return fmt.Errorf("failed to init folder for storage %q: %w", target, err)
	}

	return fn(folder)
}

func ConfigureStorageFolder(storageName string) (storage.Folder, error) {
	switch storageName {
	case "all":
		return nil, fmt.Errorf("a specific storage name was expected instead of 'all'")
	case DefaultStorage:
		return internal.ConfigureFolder()
	default:
		failover, err := internal.InitFailoverStorages()
		if err != nil {
			return nil, err
		}

		for name, folder := range failover {
			if storageName != name {
				continue
			}
			return folder, nil
		}

		available := []string{DefaultStorage}
		for name := range failover {
			available = append(available, name)
		}
		return nil, fmt.Errorf("storage with name %q is not found, available storages: %v", storageName, available)
	}
}
