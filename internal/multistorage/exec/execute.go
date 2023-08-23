package exec

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func OnAllStorages(fn func(folder storage.Folder) error) error {
	failover, err := internal.InitFailoverStorages()
	if err != nil {
		return err
	}

	folder, err := internal.ConfigureFolder()
	if err != nil {
		return err
	}
	toRun, err := cache.NameAndOrderFolders(folder, failover)
	if err != nil {
		return err
	}

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

func OnStorage(name string, fn func(folder storage.Folder) error) error {
	if name == consts.AllStorages {
		return OnAllStorages(fn)
	}

	folder, err := ConfigureStorage(name)
	if err != nil {
		return fmt.Errorf("failed to init folder for storage %q: %w", name, err)
	}

	return fn(folder)
}

func ConfigureStorage(name string) (storage.Folder, error) {
	switch name {
	case consts.AllStorages:
		return nil, fmt.Errorf("a specific storage name was expected instead of 'all'")
	case consts.DefaultStorage:
		return internal.ConfigureFolder()
	default:
		failover, err := internal.InitFailoverStorages()
		if err != nil {
			return nil, err
		}

		for n, folder := range failover {
			if name != n {
				continue
			}
			return folder, nil
		}

		available := []string{consts.DefaultStorage}
		for n := range failover {
			available = append(available, n)
		}
		return nil, fmt.Errorf("storage with name %q is not found, available storages: %v", name, available)
	}
}
