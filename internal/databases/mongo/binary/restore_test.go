package binary

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func recordingReconfigPhases(calls *[]string, failingPhase string) reconfigPhases {
	phase := func(name string) func() error {
		return func() error {
			*calls = append(*calls, name)
			if name == failingPhase {
				return errors.New(name + " failed")
			}
			return nil
		}
	}
	return reconfigPhases{
		fixReplicationState: phase("fixReplicationState"),
		recoverFromOplog:    phase("recoverFromOplog"),
		replayOplog:         phase("replayOplog"),
		fixShardingMetadata: phase("fixShardingMetadata"),
	}
}

func TestRunReconfigPhasesFixesShardingMetadataAfterOplogReplay(t *testing.T) {
	var calls []string

	require.NoError(t, runReconfigPhases(recordingReconfigPhases(&calls, ""), true))
	require.Equal(t, []string{"fixReplicationState", "recoverFromOplog", "replayOplog", "fixShardingMetadata"}, calls)
}

func TestRunReconfigPhasesWithoutPitrFixesShardingMetadataAfterRecovery(t *testing.T) {
	var calls []string

	require.NoError(t, runReconfigPhases(recordingReconfigPhases(&calls, ""), false))
	require.Equal(t, []string{"fixReplicationState", "recoverFromOplog", "fixShardingMetadata"}, calls)
}

func TestRunReconfigPhasesStopsOnFailedPhase(t *testing.T) {
	var calls []string

	err := runReconfigPhases(recordingReconfigPhases(&calls, "replayOplog"), true)
	require.ErrorContains(t, err, "replayOplog failed")
	require.Equal(t, []string{"fixReplicationState", "recoverFromOplog", "replayOplog"}, calls)
}
