//go:build !walg_integration_test

package stages

import (
	"context"
	"testing"
)

func TestProductionReplayIgnoresTestBarrier(t *testing.T) {
	t.Setenv("WALG_TEST_REPLAY_CHECKPOINT_DIR", "does-not-exist")
	if err := replayCheckpointTestBarrier(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}
