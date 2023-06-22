package multistorage

import (
	"context"
	"fmt"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

type aliveStorageCheckRes struct {
	err error
	idx int
}

// TODO: Unit tests
func FindAliveStorages(toCheck []FailoverFolder, stopOnDefaultOk bool) (ok []FailoverFolder, err error) {
	checkTimeout, err := internal.GetDurationSetting(internal.PgFailoverStoragesCheckTimeout)
	if err != nil {
		return nil, fmt.Errorf("check timeout setting: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()

	resCh := make(chan aliveStorageCheckRes, len(toCheck))
	for idx := range toCheck {
		i := idx
		go func() {
			err := checkStorageAlive(ctx, toCheck[i])
			if err != nil {
				resCh <- aliveStorageCheckRes{
					err: fmt.Errorf("storage '%s' read check: %v", toCheck[i].Name, err),
					idx: i,
				}
				return
			}

			if toCheck[i].Name == DefaultStorage && stopOnDefaultOk {
				// stop checking other storages if default is OK
				cancel()
			}

			resCh <- aliveStorageCheckRes{
				err: nil,
				idx: i,
			}
		}()
	}

	okIndexes := make(map[int]bool)
	for range toCheck {
		res := <-resCh
		if res.err == nil {
			okIndexes[res.idx] = true
			continue
		}
		tracelog.ErrorLogger.Print(err)
	}

	if len(okIndexes) == 0 {
		return nil, fmt.Errorf("no readable storages found, all %d failed", len(toCheck))
	}

	for idx := range toCheck {
		if okIndexes[idx] {
			ok = append(ok, toCheck[idx])
		}
	}

	tracelog.DebugLogger.Printf("Found %d alive storages: %v", len(ok), ok)
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
