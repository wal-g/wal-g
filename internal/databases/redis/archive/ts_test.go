package archive

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestEnrichWithAttachedTS(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	mainBackup := Backup{BackupName: "stream_20260721T120000Z", BackupType: RDBBackupType}
	tsBackup := Backup{
		BackupName:   AttachedTSDataPrefix(mainBackup.BackupName),
		BackupType:   TSBackupType,
		TSBackupID:   "ts-id",
		TSBackupPath: "/var/lib/redis/ext/ts-id",
		TSDataSize:   42,
		TSFileCount:  3,
		TSStartTime:  time.Unix(1, 0).UTC(),
		TSFinishTime: time.Unix(2, 0).UTC(),
	}
	serialized, err := json.Marshal(tsBackup)
	require.NoError(t, err)
	require.NoError(t, folder.GetSubFolder(utility.BaseBackupPath).PutObject(
		t.Context(), AttachedTSSentinelName(mainBackup.BackupName), bytes.NewReader(serialized),
	))

	require.NoError(t, EnrichWithAttachedTS(t.Context(), folder, &mainBackup))

	assert.True(t, mainBackup.HasTS)
	assert.Equal(t, tsBackup.TSBackupID, mainBackup.TSBackupID)
	assert.Equal(t, tsBackup.TSBackupPath, mainBackup.TSBackupPath)
	assert.Equal(t, tsBackup.TSDataSize, mainBackup.TSDataSize)
	assert.Equal(t, tsBackup.TSFileCount, mainBackup.TSFileCount)
	assert.Equal(t, tsBackup.TSStartTime, mainBackup.TSStartTime)
	assert.Equal(t, tsBackup.TSFinishTime, mainBackup.TSFinishTime)
}

func TestEnrichWithAttachedTSWithoutAttachedSentinel(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	backup := Backup{BackupName: "stream_20260721T120000Z", BackupType: RDBBackupType}

	require.NoError(t, EnrichWithAttachedTS(t.Context(), folder, &backup))

	assert.False(t, backup.HasTS)
}

func TestEnrichWithAttachedTSStandaloneBackup(t *testing.T) {
	backup := Backup{BackupName: "ts_20260721T120000Z", BackupType: TSBackupType}

	require.NoError(t, EnrichWithAttachedTS(t.Context(), testtools.MakeDefaultInMemoryStorageFolder(), &backup))

	assert.True(t, backup.HasTS)
}
