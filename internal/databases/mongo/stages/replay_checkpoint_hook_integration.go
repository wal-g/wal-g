//go:build walg_integration_test

package stages

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var replayCheckpointTestOnce sync.Once

// Pause only the first non-final durable checkpoint. The test kills mongod
// before releasing WAL-G, so replay cannot finish before fault injection.
func replayCheckpointTestBarrier(ctx context.Context, progress *ReplayProgress) error {
	dir := os.Getenv("WALG_TEST_REPLAY_CHECKPOINT_DIR")
	if dir == "" || progress.Snapshot().Generation == 0 {
		return nil
	}
	var err error
	replayCheckpointTestOnce.Do(func() { err = waitForReplayCheckpointRelease(ctx, dir) })
	return err
}

func waitForReplayCheckpointRelease(ctx context.Context, dir string) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0600); err != nil {
		return fmt.Errorf("publish test replay checkpoint: %w", err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if info, err := os.Stat(filepath.Join(dir, "continue")); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("test replay checkpoint release is not a regular file")
			}
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("read test replay checkpoint release: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for test replay checkpoint release: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
