package redis

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateBackupType(t *testing.T) {
	previousType := backupType
	t.Cleanup(func() { backupType = previousType })

	for _, kind := range []string{rdbBackupType, aofBackupType, rdbTSBackupType, aofTSBackupType, tsBackupType} {
		backupType = kind
		assert.NoError(t, validateBackupType(nil, nil), kind)
	}

	backupType = "invalid"
	assert.ErrorContains(t, validateBackupType(nil, nil), "invalid --type value")
}

func TestValidateTSBackupPushInput(t *testing.T) {
	previousType, previousPath, previousID := backupType, tsBackup, tsBackupID
	t.Cleanup(func() {
		backupType, tsBackup, tsBackupID = previousType, previousPath, previousID
	})

	tsBackup = t.TempDir()
	tsBackupID = "ts-id"
	backupType = rdbBackupType
	assert.ErrorContains(t, validateTSBackupPushInput(), "only valid for tiered-storage")

	backupType = tsBackupType
	tsBackup = ""
	assert.ErrorContains(t, validateTSBackupPushInput(), "is required")

	tsBackup = t.TempDir()
	assert.ErrorContains(t, validateTSBackupPushInput(), "is empty")

	tsBackup = t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tsBackup, "data"), []byte("content"), 0o600))
	assert.NoError(t, validateTSBackupPushInput())
}

func TestValidateBackupFetchRequiresAOFRedisVersion(t *testing.T) {
	previousType, previousVersion, previousPath := backupType, redisVersion, tsFetchBackup
	t.Cleanup(func() {
		backupType, redisVersion, tsFetchBackup = previousType, previousVersion, previousPath
	})

	backupType = aofBackupType
	redisVersion = ""
	tsFetchBackup = ""
	assert.ErrorContains(t, validateBackupFetch(nil, nil), "--redis-version is required")

	backupType = aofTSBackupType
	redisVersion = ""
	tsFetchBackup = t.TempDir()
	assert.ErrorContains(t, validateBackupFetch(nil, nil), "--redis-version is required")
}
