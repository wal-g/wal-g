package postgres

import (
	"bytes"
	"strings"
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

func TestHandleJournalInfo(t *testing.T) {
	// handleJournalInfo stamps CurrentBackupEnd using real wall-clock time
	// (utility.TimeNowCrossPlatformUTC), so the folder's LastModified clock must be real too.
	newFolder := func() storage.Folder {
		return memory.NewFolder("", memory.NewKVS())
	}

	t.Run("does nothing when count journals is disabled", func(t *testing.T) {
		folder := newFolder()

		bh := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000001"},
			Arguments:     BackupArguments{countJournals: false},
		}
		bh.handleJournalInfo(t.Context(), folder)

		assert.False(t, journalInfoExists(t, folder, bh.CurBackupInfo.Name))
	})

	t.Run("does nothing for permanent backups", func(t *testing.T) {
		folder := newFolder()

		bh := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000001"},
			Arguments:     BackupArguments{countJournals: true, isPermanent: true},
		}
		bh.handleJournalInfo(t.Context(), folder)

		assert.False(t, journalInfoExists(t, folder, bh.CurBackupInfo.Name))
	})

	t.Run("computes WAL volume accumulated between two backups", func(t *testing.T) {
		folder := newFolder()

		bh1 := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000001"},
			Arguments:     BackupArguments{countJournals: true},
		}
		bh1.handleJournalInfo(t.Context(), folder)
		require.True(t, journalInfoExists(t, folder, bh1.CurBackupInfo.Name))

		// WAL segments archived between the two backups; storage object size is what
		// should end up counted (i.e. compressed size, not any uncompressed estimate).
		time.Sleep(time.Millisecond)
		err := folder.GetSubFolder(utility.WalPath).PutObject(
			t.Context(), "000000010000000000000001.lz4", bytes.NewBufferString(strings.Repeat("a", 5)))
		require.NoError(t, err)

		time.Sleep(time.Millisecond)
		err = folder.GetSubFolder(utility.WalPath).PutObject(
			t.Context(), "000000010000000000000002.lz4", bytes.NewBufferString(strings.Repeat("a", 10)))
		require.NoError(t, err)

		time.Sleep(time.Millisecond)
		bh2 := &BackupHandler{
			CurBackupInfo: CurBackupInfo{Name: "base_000000010000000000000003"},
			Arguments:     BackupArguments{countJournals: true},
		}
		bh2.handleJournalInfo(t.Context(), folder)

		ji1, err := internal.NewJournalInfo(t.Context(), bh1.CurBackupInfo.Name, folder, utility.WalPath)
		require.NoError(t, err)
		assert.Equal(t, int64(15), ji1.SizeToNextBackup)

		ji2, err := internal.NewJournalInfo(t.Context(), bh2.CurBackupInfo.Name, folder, utility.WalPath)
		require.NoError(t, err)
		assert.Equal(t, int64(0), ji2.SizeToNextBackup)
	})
}
