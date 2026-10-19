//go:build !walg_integration_test

package stages

import "context"

// Production binaries have no checkpoint barrier or test environment settings.
func replayCheckpointTestBarrier(context.Context, *ReplayProgress) error { return nil }
