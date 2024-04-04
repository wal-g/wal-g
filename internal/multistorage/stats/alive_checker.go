package stats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const checkObjectName = "wal-g_storage_check"

func NewRWAliveChecker(folders map[string]storage.Folder, timeout time.Duration, writeSize uint) *AliveChecker {
	return &AliveChecker{
		folders: folders,
		timeout: timeout,
		checks: []storageCheck{&readCheck{}, &writeCheck{
			writeSize: writeSize,
		}},
	}
}

func NewROAliveChecker(folders map[string]storage.Folder, timeout time.Duration) *AliveChecker {
	return &AliveChecker{
		folders: folders,
		timeout: timeout,
		checks:  []storageCheck{&readCheck{}},
	}
}

type AliveChecker struct {
	// folders that can be checked by this checker, matched by storage names.
	folders map[string]storage.Folder
	timeout time.Duration
	checks  []storageCheck
}

func (ac *AliveChecker) CheckForAlive(storageNames ...string) map[string]bool {
	ctx, cancel := context.WithTimeout(context.Background(), ac.timeout)
	defer cancel()

	resCh := make(chan checkRes, len(storageNames))
	for _, name := range storageNames {
		folder, ok := ac.folders[name]
		if !ok {
			resCh <- checkRes{
				name: name,
				err:  fmt.Errorf("unknown storage '%s'", name),
			}
		}
		go func(folder storage.Folder, name string) {
			err := ac.checkStorage(ctx, folder)
			if err != nil {
				resCh <- checkRes{
					name: name,
					err:  fmt.Errorf("storage '%s': %v", name, err),
				}
				return
			}
			resCh <- checkRes{
				name: name,
				err:  nil,
			}
		}(folder, name)
	}

	aliveCount := 0
	results := make(map[string]bool, len(storageNames))
	for range storageNames {
		res := <-resCh
		if res.err == nil {
			results[res.name] = true
			aliveCount++
			continue
		}
		results[res.name] = false
		tracelog.ErrorLogger.Print(res.err)
	}

	tracelog.DebugLogger.Printf("Found %d alive storages among requested: %v", aliveCount, results)
	return results
}

func (ac *AliveChecker) checkStorage(ctx context.Context, folder storage.Folder) error {
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
		return errors.New("alive check timeout")
	}
}

type checkRes struct {
	name string
	err  error
}

type storageCheck interface {
	Check(ctx context.Context, folder storage.Folder) error
}

type readCheck struct{}

func (rc *readCheck) Check(_ context.Context, folder storage.Folder) error {
	// We have to ignore the context.Context here as storages package
	// can not provide a ListFolderWithContext. WAL-G might block here
	// indefinitely (which is still quite unlikely).
	_, _, err := folder.ListFolder()
	if err != nil {
		return fmt.Errorf("read check: list folder %q: %w", folder.GetPath(), err)
	}
	return nil
}

type writeCheck struct {
	writeSize uint
}

func (wc *writeCheck) Check(ctx context.Context, folder storage.Folder) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	lr := io.LimitReader(r, int64(wc.writeSize))
	err := folder.PutObjectWithContext(ctx, checkObjectName, lr)
	objPath := path.Join(folder.GetPath(), checkObjectName)
	if err != nil {
		return fmt.Errorf("write check: put object %q: %w", objPath, err)
	}
	err = folder.DeleteObjects([]string{checkObjectName})
	if err != nil {
		return fmt.Errorf("write check: delete object %q: %w", objPath, err)
	}
	return nil
}
