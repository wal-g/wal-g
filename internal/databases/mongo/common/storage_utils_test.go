package common

import (
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/utility"
	"strings"
	"testing"
)

func TestDownloadLegacySentinelWithoutProtection(t *testing.T) {
	folder := memory.NewFolder("", memory.NewKVS())
	require.NoError(t, folder.PutObject(t.Context(), "legacy"+utility.SentinelSuffix, strings.NewReader(`{"BackupName":"legacy"}`)))
	for _, name := range []string{"legacy", "LATEST"} {
		b, err := DownloadSentinel(t.Context(), folder, name)
		require.NoError(t, err)
		require.Equal(t, "legacy", b.BackupName)
		require.Equal(t, LogicalBackupType, b.BackupType)
	}
}

func TestDeletionSentinelRequiresExplicitProtection(t *testing.T) {
	for _, body := range []string{`{"BackupName":"legacy"}`, `{"Permanent":null}`, `{"Permanent":"false"}`, `{`} {
		folder := memory.NewFolder("", memory.NewKVS())
		require.NoError(t, folder.PutObject(t.Context(), "legacy"+utility.SentinelSuffix, strings.NewReader(body)))
		_, err := DownloadSentinelForDeletion(t.Context(), folder, "legacy")
		require.Error(t, err, body)
	}
	for _, permanent := range []string{"true", "false"} {
		folder := memory.NewFolder("", memory.NewKVS())
		require.NoError(t, folder.PutObject(t.Context(), "backup"+utility.SentinelSuffix, strings.NewReader(`{"Permanent":`+permanent+`}`)))
		b, err := DownloadSentinelForDeletion(t.Context(), folder, "LATEST")
		require.NoError(t, err)
		require.Equal(t, "backup", b.BackupName)
		require.Equal(t, permanent == "true", b.Permanent)
	}
}
