package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

func TestParseBackupHistoryFilename(t *testing.T) {
	t.Run("valid backup history filename", func(t *testing.T) {
		// Test case for a valid filename: 000000010000000000000003.00000028.backup
		filename := "000000010000000000000003.00000028.backup"
		timelineID, logSegNo, offset, err := postgres.ParseBackupHistoryFilename(filename)

		assert.NoError(t, err)
		assert.Equal(t, uint32(1), timelineID)
		assert.Equal(t, uint64(3), logSegNo)
		assert.Equal(t, uint32(40), offset) // 0x28 in hex = 40 in decimal
	})

	t.Run("invalid filename length", func(t *testing.T) {
		// Test case for filename that's too short
		filename := "invalid.backup"
		_, _, _, err := postgres.ParseBackupHistoryFilename(filename)

		assert.Error(t, err)
	})

	t.Run("invalid file extension", func(t *testing.T) {
		// Test case for wrong file extension
		filename := "000000010000000000000003.00000028.wrong"
		_, _, _, err := postgres.ParseBackupHistoryFilename(filename)

		assert.Error(t, err)
	})

	t.Run("invalid offset format", func(t *testing.T) {
		// Test case for invalid hex offset
		filename := "000000010000000000000003.INVALID.backup"
		_, _, _, err := postgres.ParseBackupHistoryFilename(filename)

		assert.Error(t, err)
	})
}

func TestIsBackupHistoryFilename(t *testing.T) {
	t.Run("valid filename", func(t *testing.T) {
		filename := "000000010000000000000003.00000028.backup"
		assert.True(t, postgres.IsBackupHistoryFilename(filename))
	})

	t.Run("invalid filename", func(t *testing.T) {
		filename := "invalid.backup"
		assert.False(t, postgres.IsBackupHistoryFilename(filename))
	})
}

func TestGetWalFilenameFromBackupHistoryFilename(t *testing.T) {
	t.Run("valid conversion", func(t *testing.T) {
		backupFilename := "000000010000000000000003.00000028.backup"
		expectedWalFilename := "000000010000000000000003"

		walFilename, err := postgres.GetWalFilenameFromBackupHistoryFilename(backupFilename)

		assert.NoError(t, err)
		assert.Equal(t, expectedWalFilename, walFilename)
	})

	t.Run("invalid backup filename", func(t *testing.T) {
		backupFilename := "invalid.backup"

		_, err := postgres.GetWalFilenameFromBackupHistoryFilename(backupFilename)

		assert.Error(t, err)
	})
}
