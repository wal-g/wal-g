package postgres_test

import (
	"bytes"
	"encoding/json"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func putPostgresBackup(
	t *testing.T,
	folder storage.Folder,
	name string,
	sentinel postgres.BackupSentinelDto,
	metadata *postgres.ExtendedMetadataDto,
) {
	t.Helper()
	sentinelBytes, err := json.Marshal(&sentinel)
	require.NoError(t, err)
	require.NoError(t, folder.PutObject(t.Context(), path.Join(utility.BaseBackupPath, name, "part_001.tar.lz4"),
		bytes.NewBufferString("opaque-"+name)))
	require.NoError(t, folder.PutObject(t.Context(), path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(name)),
		bytes.NewReader(sentinelBytes)))
	if metadata != nil {
		metadataBytes, err := json.Marshal(metadata)
		require.NoError(t, err)
		require.NoError(t, folder.PutObject(t.Context(), path.Join(utility.BaseBackupPath, name, utility.MetadataFileName),
			bytes.NewReader(metadataBytes)))
	}
}

func TestPostgresCopyPlanIncludesIncrementalChainAndExactWal(t *testing.T) {
	postgres.SetWalSize(16)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	parent := "base_000000010000000000000001"
	child := "base_000000010000000000000004_D_000000010000000000000001"
	fullName := parent
	count := 1
	incrementLSN := postgres.LSN(postgres.WalSegmentSize)
	putPostgresBackup(t, from, parent, postgres.BackupSentinelDto{}, nil)
	putPostgresBackup(t, from, child, postgres.BackupSentinelDto{
		IncrementFrom:     &parent,
		IncrementFullName: &fullName,
		IncrementFromLSN:  &incrementLSN,
		IncrementCount:    &count,
	}, &postgres.ExtendedMetadataDto{
		StartLsn:  postgres.LSN(postgres.WalSegmentSize * 2),
		FinishLsn: postgres.LSN(postgres.WalSegmentSize * 4),
	})
	// A common-prefix backup must not leak into the selected closure.
	putPostgresBackup(t, from, child+"_extra", postgres.BackupSentinelDto{}, nil)
	for i := 1; i <= 4; i++ {
		walName := postgres.WalSegmentNo(i).GetFilename(1)
		require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, walName), bytes.NewBufferString("wal")))
	}
	walSidecar := postgres.WalSegmentNo(2).GetFilename(1) + ".partial"
	require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, walSidecar),
		bytes.NewBufferString("partial-wal")))
	require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, "00000002.history.br"),
		bytes.NewBufferString("opaque-timeline-history")))

	plan, err := postgres.BuildCopyPlan(t.Context(), from, to, child, false)
	require.NoError(t, err)
	targets := make(map[string]bool)
	for _, entry := range plan.Entries() {
		targets[entry.TargetPath] = true
		require.NotContains(t, entry.TargetPath, child+"_extra")
	}
	require.True(t, targets[path.Join(utility.BaseBackupPath, parent, "part_001.tar.lz4")])
	require.True(t, targets[path.Join(utility.BaseBackupPath, child, "part_001.tar.lz4")])
	require.True(t, targets[path.Join(utility.WalPath, postgres.WalSegmentNo(1).GetFilename(1))])
	require.True(t, targets[path.Join(utility.WalPath, postgres.WalSegmentNo(4).GetFilename(1))])
	require.True(t, targets[path.Join(utility.WalPath, walSidecar)])
	require.True(t, targets[path.Join(utility.WalPath, "00000002.history.br")])
}

func TestPostgresCopyPlanDoesNotCountWalSidecarsAsCompleteSegments(t *testing.T) {
	postgres.SetWalSize(16)
	tests := []struct {
		name           string
		missingSegment int
		sidecarSuffix  string
		expectedError  string
	}{
		{name: "partial cannot satisfy first segment", missingSegment: 1, sidecarSuffix: ".partial",
			expectedError: "first required PostgreSQL WAL segment"},
		{name: "backup sidecar cannot satisfy interior gap", missingSegment: 2,
			sidecarSuffix: ".00000028.backup.br", expectedError: "gap in PostgreSQL WAL history"},
		{name: "backup sidecar cannot satisfy last segment", missingSegment: 4,
			sidecarSuffix: ".00000028.backup.br", expectedError: "last required PostgreSQL WAL segment"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			from := testtools.MakeDefaultInMemoryStorageFolder()
			to := testtools.MakeDefaultInMemoryStorageFolder()
			backupName := "base_000000010000000000000001"
			putPostgresBackup(t, from, backupName, postgres.BackupSentinelDto{}, &postgres.ExtendedMetadataDto{
				StartLsn:  postgres.LSN(postgres.WalSegmentSize * 2),
				FinishLsn: postgres.LSN(postgres.WalSegmentSize * 4),
			})

			for i := 1; i <= 4; i++ {
				walName := postgres.WalSegmentNo(i).GetFilename(1)
				if i == test.missingSegment {
					walName += test.sidecarSuffix
				} else {
					walName += ".br"
				}
				require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, walName),
					bytes.NewBufferString("wal")))
			}

			_, err := postgres.BuildCopyPlan(t.Context(), from, to, backupName, false)
			require.ErrorContains(t, err, test.expectedError)
		})
	}
}

func TestPostgresCopyAllPreservesThreeLevelIncrementalCommitOrder(t *testing.T) {
	postgres.SetWalSize(16)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	full := "base_000000010000000000000001"
	child := "base_000000010000000000000002_D_000000010000000000000001"
	grandchild := "base_000000010000000000000003_D_000000010000000000000002"
	metadata := postgres.ExtendedMetadataDto{
		StartLsn:  postgres.LSN(postgres.WalSegmentSize * 2),
		FinishLsn: postgres.LSN(postgres.WalSegmentSize * 2),
	}

	putPostgresBackup(t, from, full, postgres.BackupSentinelDto{}, &metadata)
	putPostgresBackup(t, from, child, postgres.BackupSentinelDto{IncrementFrom: &full}, &metadata)
	putPostgresBackup(t, from, grandchild, postgres.BackupSentinelDto{IncrementFrom: &child}, &metadata)
	for i := 1; i <= 2; i++ {
		walName := postgres.WalSegmentNo(i).GetFilename(1)
		require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, walName), bytes.NewBufferString("wal")))
	}

	plan, err := postgres.BuildCopyPlan(t.Context(), from, to, "", false)
	require.NoError(t, err)
	phases := make(map[string]uint16)
	for _, entry := range plan.Entries() {
		phases[entry.TargetPath] = uint16(entry.Phase)
	}
	fullSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(full))
	childSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(child))
	grandchildSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(grandchild))
	require.Less(t, phases[fullSentinel], phases[childSentinel])
	require.Less(t, phases[childSentinel], phases[grandchildSentinel])
}
