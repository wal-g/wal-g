package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeltaOriginUsesExactAttemptIdentity(t *testing.T) {
	const wal = "000000010000000000000002"
	const suffix = "_U_0123456789abcdef0123456789abcdef"
	for _, parent := range []string{
		"base_" + wal,
		"base_" + wal + suffix,
		"base_" + wal + "_D_000000010000000000000001" + suffix,
	} {
		child := BackupInfo{BackupName: "base_000000010000000000000003_D_" + wal + suffix, IncrementFrom: &parent}
		candidates := []BackupInfo{
			{BackupName: "base_" + wal + "_U_ffffffffffffffffffffffffffffffff"},
			{BackupName: parent},
		}
		require.Equal(t, parent, child.GetDeltaOriginName(candidates))
		// A filtered listing must not erase the known relationship.
		require.Equal(t, parent, child.GetDeltaOriginName(nil))
		child.IncrementFrom = nil
		require.Empty(t, child.GetDeltaOriginName(candidates))
	}
}

func TestDeltaOriginLegacyFallbackDoesNotGuess(t *testing.T) {
	parent := "base_000000010000000000000002"
	child := BackupInfo{BackupName: "base_000000010000000000000003_D_000000010000000000000002"}
	candidates := []BackupInfo{{BackupName: parent + "_U_0123456789abcdef0123456789abcdef"}, {BackupName: parent}}
	require.Equal(t, parent, child.GetDeltaOriginName(candidates))
	candidates = append(candidates, BackupInfo{BackupName: parent + "_D_000000010000000000000001"})
	require.Empty(t, child.GetDeltaOriginName(candidates))
}
