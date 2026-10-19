package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

type historyCopyFolder struct {
	storage.Folder
	onRead func(string) error
}

func (folder *historyCopyFolder) ReadObject(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := folder.onRead(name); err != nil {
		return nil, err
	}
	return folder.Folder.ReadObject(ctx, name)
}

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

func TestPostgresCopyPlanFiltersTimelineHistoryWithoutReadingIt(t *testing.T) {
	postgres.SetWalSize(16)
	for _, withHistory := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "with history"}[withHistory], func(t *testing.T) {
			from := testtools.MakeDefaultInMemoryStorageFolder()
			to := testtools.MakeDefaultInMemoryStorageFolder()
			backupName := "base_0000000A0000000000000001"
			putPostgresBackup(t, from, backupName, postgres.BackupSentinelDto{}, &postgres.ExtendedMetadataDto{
				StartLsn: postgres.LSN(postgres.WalSegmentSize + 1), FinishLsn: postgres.LSN(postgres.WalSegmentSize + 2),
			})
			require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, postgres.WalSegmentNo(1).GetFilename(10)),
				bytes.NewBufferString("wal")))

			wanted := make(map[string]bool)
			for name, keep := range map[string]bool{
				"00000002.history": false, "00000009.history": false,
				"0000000A.history": true, "0000000a.history": true,
				"0000000B.history": true, "0000000b.history": true, "0000000C.history": true,
				"00000000.history": true, "2.history": true, "000000002.history": true,
				"invalid.history": true, "00000002.history.history": true,
			} {
				for _, suffix := range []string{"", ".br", ".gz", ".lz4", ".lzma", ".lzo", ".zst"} {
					object := path.Join(utility.WalPath, name+suffix)
					wanted[object] = keep
					require.NoError(t, from.PutObject(t.Context(), object, bytes.NewBufferString("opaque-history")))
				}
			}
			var historyReads atomic.Int64
			tracked := &historyCopyFolder{Folder: from, onRead: func(name string) error {
				if keep, ok := wanted[name]; ok {
					historyReads.Add(1)
					if !keep {
						return io.ErrUnexpectedEOF
					}
				}
				return nil
			}}
			plan, err := postgres.BuildCopyPlan(t.Context(), tracked, to, backupName, withHistory)
			require.NoError(t, err)
			require.Zero(t, historyReads.Load(), "planning must not open history payloads")
			retries := 0
			require.NoError(t, copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{MaxRetries: &retries}))
			var expectedReads int64
			for name, keep := range wanted {
				exists, err := to.Exists(t.Context(), name)
				require.NoError(t, err)
				require.Equal(t, keep, exists, name)
				if keep {
					expectedReads++
				}
			}
			require.Equal(t, expectedReads, historyReads.Load(), "excluded histories must never be read")
		})
	}
}

func TestPostgresCopyAllUnionsTimelineHistory(t *testing.T) {
	postgres.SetWalSize(16)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	metadata := postgres.ExtendedMetadataDto{
		StartLsn: postgres.LSN(postgres.WalSegmentSize + 1), FinishLsn: postgres.LSN(postgres.WalSegmentSize + 2),
	}
	for _, timeline := range []uint32{10, 12} {
		wal := postgres.WalSegmentNo(1).GetFilename(timeline)
		putPostgresBackup(t, from, "base_"+wal, postgres.BackupSentinelDto{}, &metadata)
		require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, wal), bytes.NewBufferString("wal")))
	}
	for _, name := range []string{"00000009.history", "0000000A.history", "0000000b.history", "0000000C.history"} {
		require.NoError(t, from.PutObject(t.Context(), path.Join(utility.WalPath, name), bytes.NewBufferString("opaque")))
	}
	for _, tc := range []struct {
		name, backup string
		want         []string
	}{
		{"all", "", []string{"0000000A.history", "0000000b.history", "0000000C.history"}},
		{"newest only", "base_0000000C0000000000000001", []string{"0000000C.history"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := postgres.BuildCopyPlan(t.Context(), from, testtools.MakeDefaultInMemoryStorageFolder(), tc.backup, false)
			require.NoError(t, err)
			var histories []string
			for _, entry := range plan.Entries() {
				if strings.HasSuffix(entry.TargetPath, ".history") {
					histories = append(histories, path.Base(entry.TargetPath))
				}
			}
			require.ElementsMatch(t, tc.want, histories)
		})
	}
}

func TestPostgresCopyHistoryFailureRequiresDisasterMode(t *testing.T) {
	postgres.SetWalSize(16)
	for _, disaster := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "disaster"}[disaster], func(t *testing.T) {
			sourceConfig, targetConfig := viper.New(), viper.New()
			require.NoError(t, sourceConfig.MergeConfigMap(map[string]any{
				conf.DisasterCopy:          disaster,
				conf.DisasterCopySkipFiles: conf.PGDefaultSettings[conf.DisasterCopySkipFiles],
			}))
			pattern, err := copyutil.DisasterCopyPattern(sourceConfig, targetConfig)
			require.NoError(t, err)

			from := testtools.MakeDefaultInMemoryStorageFolder()
			to := testtools.MakeDefaultInMemoryStorageFolder()
			backupName := "base_000000020000000000000001"
			putPostgresBackup(t, from, backupName, postgres.BackupSentinelDto{}, &postgres.ExtendedMetadataDto{
				StartLsn: postgres.LSN(postgres.WalSegmentSize + 1), FinishLsn: postgres.LSN(postgres.WalSegmentSize + 2),
			})
			wal := path.Join(utility.WalPath, postgres.WalSegmentNo(1).GetFilename(2))
			history := path.Join(utility.WalPath, "00000002.history.br")
			for _, name := range []string{wal, history} {
				require.NoError(t, from.PutObject(t.Context(), name, bytes.NewBufferString("opaque")))
			}
			var reads atomic.Int64
			tracked := &historyCopyFolder{Folder: from, onRead: func(name string) error {
				if name == history {
					reads.Add(1)
					return io.ErrUnexpectedEOF
				}
				return nil
			}}
			plan, err := postgres.BuildCopyPlan(t.Context(), tracked, to, backupName, false)
			require.NoError(t, err)
			retries := 1
			err = copyutil.Execute(t.Context(), plan, copyutil.ExecuteOptions{MaxRetries: &retries, SkipErrorsFor: pattern})
			if disaster {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
			require.Equal(t, int64(2), reads.Load(), "retry before deciding whether to skip")
			exists, err := to.Exists(t.Context(), path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName)))
			require.NoError(t, err)
			require.Equal(t, disaster, exists, "only disaster mode may publish after a failed history transfer")
			exists, err = to.Exists(t.Context(), history)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
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
	entries := make(map[string]copyutil.Entry)
	for _, entry := range plan.Entries() {
		entries[entry.TargetPath] = entry
	}
	fullSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(full))
	childSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(child))
	grandchildSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(grandchild))
	require.Equal(t, copyutil.BackupCommitPhase, entries[fullSentinel].Phase)
	require.Equal(t, copyutil.BackupCommitPhase, entries[childSentinel].Phase)
	require.Equal(t, copyutil.BackupCommitPhase, entries[grandchildSentinel].Phase)
	require.Less(t, entries[fullSentinel].Sequence, entries[childSentinel].Sequence)
	require.Less(t, entries[childSentinel].Sequence, entries[grandchildSentinel].Sequence)
}
