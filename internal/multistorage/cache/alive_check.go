package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/wal-g/tracelog"
)

type checkRes struct {
	key key
	err error
}

func checkForAlive(timeout time.Duration, storages ...NamedFolder) map[key]bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resCh := make(chan checkRes, len(storages))
	for _, stor := range storages {
		go func(s NamedFolder) {
			err := checkStorage(ctx, s)
			if err != nil {
				resCh <- checkRes{
					key: s.Key,
					err: fmt.Errorf("storage '%s' read check: %v", s.Name, err),
				}
				return
			}
			resCh <- checkRes{
				key: s.Key,
				err: nil,
			}
		}(stor)
	}

	aliveCount := 0
	results := make(map[key]bool, len(storages))
	for range storages {
		res := <-resCh
		if res.err == nil {
			results[res.key] = true
			aliveCount++
			continue
		}
		results[res.key] = false
		tracelog.ErrorLogger.Print(res.err)
	}

	tracelog.DebugLogger.Printf("Found %d alive storages: %v", aliveCount, results)
	return results
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
