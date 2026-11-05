package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/fs"
)

func TestRecoveryPermanentRestorePointCannotOverflow(t *testing.T) {
	seg := &deletionSegment{backups: map[string]internal.DeletionBackup{"B1": {
		Name: "B1", Permanent: true,
		Metadata: postgres.DeletionBackupMetadata{LogsKnown: true, Meta: postgres.ExtendedMetadataDto{StartLsn: 1, FinishLsn: 2}},
	}}}
	err := extendSegmentProtection(t.Context(), fs.NewFolder(t.TempDir(), ""), &BackupSentinelDto{},
		SegmentMetadata{RestorePointLSN: "FFFFFFFF/FFFFFFFF"}, "B1", seg)
	require.NoError(t, err)
	data := seg.backups["B1"].Metadata.(postgres.DeletionBackupMetadata)
	require.False(t, data.LogsKnown)
	require.Equal(t, postgres.LSN(2), data.Meta.FinishLsn)
}
