package exec

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func OnAllStorages(fn func(folder storage.Folder) error) error {
	failover, err := internal.ConfigureFailoverStorages()
	if err != nil {
		return err
	}

	primary, err := internal.ConfigureStorage()
	if err != nil {
		return err
	}
	toRun := multistorage.NameAndOrderStorages(primary, failover)

	atLeastOneOK := false
	for _, st := range toRun {
		tracelog.InfoLogger.Printf("storage %s", st.Name)
		err := fn(st.RootFolder())
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

	st, err := ConfigureStorage(name)
	if err != nil {
		return fmt.Errorf("failed to init folder for storage %q: %w", name, err)
	}

	return fn(st.RootFolder())
}

func ConfigureStorage(name string) (storage.Storage, error) {
	switch name {
	case consts.AllStorages:
		return nil, fmt.Errorf("a specific storage name was expected instead of 'all'")
	case consts.DefaultStorage:
		return internal.ConfigureStorage()
	default:
		failover, err := internal.ConfigureFailoverStorages()
		if err != nil {
			return nil, err
		}

		st, found := failover[name]
		if found {
			return st, nil
		}

		available := []string{consts.DefaultStorage}
		for n := range failover {
			available = append(available, n)
		}
		return nil, fmt.Errorf("storage with name %q is not found, available storages: %v", name, available)
	}
}
