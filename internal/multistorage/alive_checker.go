package multistorage

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

// TODO: Unit tests
func FindAliveStorages(toCheck []FailoverFolder, stopOnDefaultOk bool) (ok []FailoverFolder, err error) {
	checkTimeout, err := internal.GetDurationSetting(internal.PgFailoverStoragesCheckTimeout)
	if err != nil {
		return nil, fmt.Errorf("check timeout setting: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()

	okFolderCh := make(chan FailoverFolder, len(toCheck))
	errCh := make(chan error, len(toCheck))

	for idx := range toCheck {
		i := idx
		go func() {
			err := checkStorageAlive(ctx, toCheck[i])
			if err != nil {
				errCh <- fmt.Errorf("storage '%s' read check: %v", toCheck[i].Name, err)
				return
			}

			if toCheck[i].Name == DefaultStorage && stopOnDefaultOk {
				// stop checking other storages if default is OK
				cancel()
			}

			okFolderCh <- toCheck[i]
		}()
	}

	checkedCount := 0
	for checkedCount < len(toCheck) {
		select {
		case okFolder := <-okFolderCh:
			ok = append(ok, okFolder)
		case err := <-errCh:
			tracelog.ErrorLogger.Print(err)
		}
		checkedCount++
	}

	if len(ok) == 0 {
		return nil, fmt.Errorf("no readable storages found, all %d failed", checkedCount)
	}

	return ok, nil
}

func checkStorageAlive(ctx context.Context, folder FailoverFolder) error {
	// currently, we use simple ListFolder() call to check if storage is up and reachable
	errCh := make(chan error, 1)
	go func() {
		_, _, err := folder.ListFolder()
		errCh <- err
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return fmt.Errorf("storage '%s' read check timeout", folder.Name)
	}
}
