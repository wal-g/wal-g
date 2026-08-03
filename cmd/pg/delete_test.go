package pg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func journalInfoExists(t *testing.T, folder storage.Folder, backupName string) bool {
	t.Helper()
	exists, err := folder.GetSubFolder(utility.BaseBackupPath).Exists(t.Context(), internal.JournalPrefix+backupName)
	require.NoError(t, err)
	return exists
}

func TestDeleteJournalInfo(t *testing.T) {
	t.Run("no journal for backup does not panic", func(t *testing.T) {
		folder := memory.NewFolder("", memory.NewKVS())

		assert.NotPanics(t, func() {
			deleteJournalInfo(t.Context(), folder, "base_000000010000000000000001", true)
		})
	})

	t.Run("dry run leaves journal in place", func(t *testing.T) {
		folder := memory.NewFolder("", memory.NewKVS())
		backupName := "base_000000010000000000000001"

		ji := internal.NewEmptyJournalInfo(backupName, time.Time{}, time.Now(), utility.WalPath)
		require.NoError(t, ji.Upload(t.Context(), folder))
		require.True(t, journalInfoExists(t, folder, backupName))

		deleteJournalInfo(t.Context(), folder, backupName, false)

		assert.True(t, journalInfoExists(t, folder, backupName))
	})

	t.Run("confirmed run deletes the journal", func(t *testing.T) {
		folder := memory.NewFolder("", memory.NewKVS())
		backupName := "base_000000010000000000000001"

		ji := internal.NewEmptyJournalInfo(backupName, time.Time{}, time.Now(), utility.WalPath)
		require.NoError(t, ji.Upload(t.Context(), folder))
		require.True(t, journalInfoExists(t, folder, backupName))

		deleteJournalInfo(t.Context(), folder, backupName, true)

		assert.False(t, journalInfoExists(t, folder, backupName))
	})
}
