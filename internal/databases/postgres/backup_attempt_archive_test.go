package postgres_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestBackupAttemptsMixedArchiveCopyAndDelete(t *testing.T) {
	postgres.SetWalSize(16)
	root := testtools.MakeDefaultInMemoryStorageFolder()
	legacy := "base_000000010000000000000002"
	first := legacy + "_U_11111111111111111111111111111111"
	second := legacy + "_U_22222222222222222222222222222222"
	child := "base_000000010000000000000003_D_000000010000000000000002_U_33333333333333333333333333333333"
	grandchild := "base_000000010000000000000004_D_000000010000000000000003_U_44444444444444444444444444444444"
	meta := &postgres.ExtendedMetadataDto{StartLsn: postgres.LSN(postgres.WalSegmentSize * 2), FinishLsn: postgres.LSN(postgres.WalSegmentSize * 4)}
	for _, name := range []string{legacy, first, second} {
		putPostgresBackup(t, root, name, postgres.BackupSentinelDto{}, meta)
	}
	count, lsn := 1, postgres.LSN(postgres.WalSegmentSize*2)
	for name, parent := range map[string]string{child: first, grandchild: child} {
		putPostgresBackup(t, root, name, postgres.BackupSentinelDto{
			IncrementFrom: &parent, IncrementFullName: &first, IncrementFromLSN: &lsn, IncrementCount: &count,
		}, meta)
	}
	listed, err := internal.GetBackups(t.Context(), root.GetSubFolder(utility.BaseBackupPath))
	require.NoError(t, err)
	require.Len(t, listed, 5)
	for i := 1; i <= 4; i++ {
		require.NoError(t, root.PutObject(t.Context(), utility.WalPath+postgres.WalSegmentNo(i).GetFilename(1), strings.NewReader("wal")))
	}

	// Copy must follow exact DeltaFrom identities, not another attempt with the
	// same WAL prefix, for both full and delta parents in a mixed archive.
	plan, err := postgres.BuildCopyPlan(t.Context(), root, testtools.MakeDefaultInMemoryStorageFolder(), grandchild, true)
	require.NoError(t, err)
	paths := make(map[string]bool)
	for _, entry := range plan.Entries() {
		paths[entry.SourcePath] = true
	}
	for _, name := range []string{first, child, grandchild} {
		require.True(t, paths[utility.BaseBackupPath+internal.SentinelNameFromBackup(name)])
	}
	for _, name := range []string{legacy, second} {
		require.False(t, paths[utility.BaseBackupPath+internal.SentinelNameFromBackup(name)])
	}

	deleter, err := postgres.NewDeleteHandler(t.Context(), root, nil, nil, false)
	require.NoError(t, err)
	target, err := deleter.FindTargetByName(first)
	require.NoError(t, err)
	require.NoError(t, deleter.DeleteTarget(t.Context(), target, true, false, func(string) bool { return true }))
	for _, name := range []string{legacy, first, second, child, grandchild} {
		backup, err := postgres.NewBackup(root.GetSubFolder(utility.BaseBackupPath), name)
		require.NoError(t, err)
		exists, err := backup.CheckExistence(t.Context())
		require.NoError(t, err)
		require.Equal(t, name == legacy || name == second, exists, name)
		dataExists, err := root.Exists(t.Context(), utility.BaseBackupPath+name+"/part_001.tar.lz4")
		require.NoError(t, err)
		require.Equal(t, exists, dataExists, name)
	}
}
