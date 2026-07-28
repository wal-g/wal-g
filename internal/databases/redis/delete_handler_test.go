package redis_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestPurgeRetainCountsAttachedTSWithMainBackup(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	mainName := "stream_20260721T120000Z"
	standaloneName := "ts_20260721T120001Z"
	baseTime := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)

	putSentinel := func(name string, backup archive.Backup) {
		t.Helper()
		serialized, err := json.Marshal(backup)
		require.NoError(t, err)
		require.NoError(t, folder.PutObject(t.Context(), name, bytes.NewReader(serialized)))
	}

	putSentinel(mainName+utility.SentinelSuffix, archive.Backup{
		BackupName:      mainName,
		BackupType:      archive.RDBBackupType,
		StartLocalTime:  baseTime,
		FinishLocalTime: baseTime,
	})
	putSentinel(standaloneName+utility.SentinelSuffix, archive.Backup{
		BackupName:      standaloneName,
		BackupType:      archive.TSBackupType,
		StartLocalTime:  baseTime.Add(time.Minute),
		FinishLocalTime: baseTime.Add(time.Minute),
	})

	require.NoError(t, folder.PutObject(t.Context(), mainName+"/stream.br", bytes.NewReader([]byte("main"))))
	require.NoError(t, folder.PutObject(t.Context(), mainName+"/ts_data/part.tar", bytes.NewReader([]byte("ts"))))
	attachedTSSentinel, err := json.Marshal(archive.Backup{
		BackupName:  archive.AttachedTSDataPrefix(mainName),
		BackupType:  archive.TSBackupType,
		TSBackupID:  "attached-ts",
		TSFileCount: 1,
	})
	require.NoError(t, err)
	require.NoError(t, folder.GetSubFolder(utility.BaseBackupPath).PutObject(
		t.Context(), archive.AttachedTSSentinelName(mainName), bytes.NewReader(attachedTSSentinel),
	))

	backupTimes, err := internal.GetBackups(t.Context(), folder)
	require.NoError(t, err)
	require.Len(t, backupTimes, 2, "the nested TS sentinel must not be discovered as a top-level backup")

	details, err := redisdb.GetBackupDetails(t.Context(), folder, backupTimes)
	require.NoError(t, err)
	require.Len(t, details, 2)
	for _, detail := range details {
		if detail.BackupName == mainName {
			assert.True(t, detail.HasTS)
		}
	}

	require.NoError(t, redisdb.HandlePurge(
		t.Context(), folder, redisdb.PurgeRetainCount(1), redisdb.PurgeDryRun(false),
	))

	backupTimes, err = internal.GetBackups(t.Context(), folder)
	require.NoError(t, err)
	require.Len(t, backupTimes, 1)
	assert.Equal(t, standaloneName, backupTimes[0].BackupName)

	mainFolder := folder.GetSubFolder(mainName)
	mainObjects, mainFolders, err := mainFolder.ListFolder(t.Context())
	require.NoError(t, err)
	assert.Empty(t, mainObjects)
	assert.Empty(t, mainFolders)

	standaloneExists, err := folder.Exists(t.Context(), standaloneName+utility.SentinelSuffix)
	require.NoError(t, err)
	assert.True(t, standaloneExists)
}
