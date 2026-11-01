//go:build walg_integration_test

package stages

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplayCheckpointBarrierWaitsForRelease(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- waitForReplayCheckpointRelease(ctx, dir) }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("checkpoint was not published")
		case <-ticker.C:
		}
	}
	select {
	case err := <-done:
		t.Fatalf("barrier returned before release: %v", err)
	default:
	}
	if err := os.WriteFile(filepath.Join(dir, "continue"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReplayCheckpointBarrierCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForReplayCheckpointRelease(ctx, t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestReplayCheckpointBarrierInvalidDirectory(t *testing.T) {
	err := waitForReplayCheckpointRelease(context.Background(), filepath.Join(t.TempDir(), "missing"))
	if err == nil {
		t.Fatal("expected failure to publish checkpoint")
	}
}

func TestReplayCheckpointBarrierRejectsDirectoryAsRelease(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "continue"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := waitForReplayCheckpointRelease(context.Background(), dir); err == nil {
		t.Fatal("expected invalid release file error")
	}
}

func TestReplayCheckpointBarrierSkipsInitialProgress(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("WALG_TEST_REPLAY_CHECKPOINT_DIR", dir)
	if err := replayCheckpointTestBarrier(context.Background(), &ReplayProgress{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ready")); !os.IsNotExist(err) {
		t.Fatalf("initial progress must not publish a checkpoint: %v", err)
	}
}
