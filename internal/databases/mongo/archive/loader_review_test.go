package archive

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/utility"
)

func TestLegacyBackupMetadataReadAndDeletionAreSeparate(t *testing.T) {
	base := memory.NewFolder("", memory.NewKVS())
	require.NoError(t, base.PutObject(t.Context(), "legacy"+utility.SentinelSuffix, strings.NewReader(`{"BackupName":"legacy"}`)))
	dl := &StorageDownloader{backupsFolder: base}
	b, err := dl.BackupMeta(t.Context(), "legacy")
	require.NoError(t, err)
	require.Equal(t, "legacy", b.Name())
	backups, err := dl.LoadBackups(t.Context(), []string{"legacy"})
	require.NoError(t, err)
	require.Len(t, backups, 1)
	_, err = dl.BackupMetaForDeletion(t.Context(), "legacy")
	require.ErrorContains(t, err, "unknown protection")
	require.NoError(t, base.PutObject(t.Context(), "legacy"+utility.SentinelSuffix, strings.NewReader(`{"Permanent":false}`)))
	b, err = dl.BackupMetaForDeletion(t.Context(), "LATEST")
	require.NoError(t, err)
	require.Equal(t, "legacy", b.Name())
}
