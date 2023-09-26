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

func NewRWAliveChecker(timeout time.Duration, writeSize uint32) AliveChecker {
	return AliveChecker{
		timeout: timeout,
		checks: []storageCheck{&readCheck{}, &writeCheck{
			writeSize: writeSize,
		}},
	}
}

func NewReadAliveChecker(timeout time.Duration) AliveChecker {
	return AliveChecker{
		timeout: timeout,
		checks:  []storageCheck{&readCheck{}},
	}
}

type AliveChecker struct {
	timeout time.Duration
	checks  []storageCheck
}

func (ac *AliveChecker) checkForAlive(storages ...NamedFolder) map[key]bool {
	ctx, cancel := context.WithTimeout(context.Background(), ac.timeout)
	defer cancel()

	resCh := make(chan checkRes, len(storages))
	for _, stor := range storages {
		go func(s NamedFolder) {
			err := ac.checkStorage(ctx, s)
			if err != nil {
				resCh <- checkRes{
					key: s.Key,
					err: fmt.Errorf("storage '%s': %v", s.Name, err),
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

func (ac *AliveChecker) checkStorage(ctx context.Context, folder NamedFolder) error {
	errCh := make(chan error, 1)
	go func() {
		for i := range ac.checks {
			errCh <- ac.checks[i].Check(ctx, folder)
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return fmt.Errorf("storage '%s' alive check timeout", folder.Name)
	}
}

type checkRes struct {
	key key
	err error
}

type storageCheck interface {
	Check(ctx context.Context, folder NamedFolder) error
}

type readCheck struct{}

func (rc *readCheck) Check(_ context.Context, folder NamedFolder) error {
	// We have to ignore the context.Context here as storages package
	// can not provide a ListFolderWithContext. WAL-G might block here
	// indefinitely (which is still quite unlikely).
	_, _, err := folder.ListFolder()
	if err != nil {
		return fmt.Errorf("read check: %w", err)
	}
	return nil
}

type writeCheck struct {
	writeSize uint32
}

func (wc *writeCheck) Check(ctx context.Context, folder NamedFolder) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	lr := io.LimitReader(r, int64(wc.writeSize))
	err := folder.PutObjectWithContext(ctx, checkObjectName, lr)
	if err != nil {
		return fmt.Errorf("write check: %w", err)
	}
	return nil
}
