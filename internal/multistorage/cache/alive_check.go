package cache

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/wal-g/tracelog"
)

const checkObjectName = "wal-g_storage_check"

type checkRes struct {
	key key
	err error
}

func checkForAlive(timeout time.Duration, size uint, storages ...NamedFolder) map[key]bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resCh := make(chan checkRes, len(storages))
	for _, stor := range storages {
		go func(s NamedFolder) {
			err := checkStorage(ctx, size, s)
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

func checkStorage(ctx context.Context, size uint, folder NamedFolder) error {
	errCh := make(chan error, 1)
	// todo simplify code using PutObjectWithContext after merge https://github.com/wal-g/wal-g/pull/1546
	// todo what about ListFolderWithContext?
	go func() {
		_, _, err := folder.ListFolder()
		if err != nil {
			errCh <- err
			return
		}
		if size > 0 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			lr := io.LimitReader(r, int64(size))
			err = folder.PutObject(checkObjectName, lr)
			if err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return fmt.Errorf("storage '%s' read check timeout", folder.Name)
	}
}
