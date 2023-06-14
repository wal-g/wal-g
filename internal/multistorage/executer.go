package multistorage

import (
	"fmt"
	"strings"

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
	if target == DefaultStorage {
		folder, err := internal.ConfigureFolder()
		if err != nil {
			return err
		}

		return fn(folder)
	}

	if target == "all" {
		return executeOnAllStorages(fn)
	}

	failover, err := internal.InitFailoverStorages()
	if err != nil {
		return err
	}

	for name := range failover {
		if target != name {
			continue
		}
		return fn(failover[name])
	}

	available := []string{DefaultStorage}
	for name := range failover {
		available = append(available, name)
	}
	return fmt.Errorf("target storage '%s' not found, available storages: %v", target, strings.Join(available, ", "))
}
