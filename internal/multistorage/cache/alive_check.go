package cache

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

type checkRes struct {
	name string
	err  error
}

func checkForAlive(storages ...NamedFolder) (map[string]bool, error) {
	checkTimeout, err := internal.GetDurationSetting(internal.PgFailoverStoragesCheckTimeout)
	if err != nil {
		return nil, fmt.Errorf("get check timeout setting: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()

	resCh := make(chan checkRes, len(storages))
	for _, storage := range storages {
		go func(s NamedFolder) {
			err := checkStorage(ctx, s)
			if err != nil {
				resCh <- checkRes{
					name: s.Name,
					err:  fmt.Errorf("storage '%s' read check: %v", s.Name, err),
				}
				return
			}
			resCh <- checkRes{
				name: s.Name,
				err:  nil,
			}
		}(storage)
	}

	aliveCount := 0
	results := make(map[string]bool, len(storages))
	for range storages {
		res := <-resCh
		if res.err == nil {
			results[res.name] = true
			aliveCount++
			continue
		}
		results[res.name] = false
		tracelog.ErrorLogger.Print(err)
	}

	tracelog.DebugLogger.Printf("Found %d alive storages: %v", aliveCount, results)
	return results, nil
}

func checkStorage(ctx context.Context, folder NamedFolder) error {
	// Currently, we use the simple ListFolder() call to check if the storage is up and reachable
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
